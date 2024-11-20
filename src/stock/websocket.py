import os
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict
import orjson
import json
import websockets
import aiohttp
from src.logger import logger
from .crud import get_company_details
from src.common.producer import init_kafka_producer, close_kafka_producer
import random
import hashlib
TOPIC_STOCK_DATA = "real_time_stock_prices"
approval_key_cache = None  # 승인 키 캐싱

# 세부 키 생성 함수
def generate_key(symbol, timestamp):
    raw_key = f"{symbol}_{timestamp}"
    hashed_key = int(hashlib.md5(raw_key.encode('utf-8')).hexdigest(), 16) % 3  # 30개의 파티션
    return f"{symbol}_{hashed_key}"

# 승인 키 가져오는 함수
async def get_approval():
    global approval_key_cache
    if approval_key_cache:
        return approval_key_cache

    url = 'https://openapivts.koreainvestment.com:29443/oauth2/Approval'
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": os.getenv("APP_KEY"),
        "secretkey": os.getenv("APP_SECRET")
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=body) as res:
                if res.status != 200:
                    logger.error(f"Failed to get approval key. Status: {res.status}")
                    return None
                data = await res.json()
                approval_key_cache = data.get("approval_key")
                if approval_key_cache:
                    logger.info("Approval key obtained successfully.")
                    return approval_key_cache
                else:
                    logger.error("Approval key not found in response.")
                    return None
    except Exception as e:
        logger.error(f"Error while getting approval key: {e}")
        return None


# WebSocket 구독 메시지 생성 함수
async def subscribe(websocket, app_key, stock_code):
    message = json.dumps({
        "header": {
            "approval_key": app_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8",
        },
        "body": {
            "input": {
                "tr_id": "H0STCNT0",
                "tr_key": stock_code
            }
        }
    })
    await websocket.send(message)
    logger.info(f"Subscribed to H0STCNT0 for stock: {stock_code}")


def process_data_for_kafka(data: str, stock_symbol: str):
    stock_info = get_company_details(stock_symbol)
    if not stock_info or "id" not in stock_info or "name" not in stock_info:
        logger.error(f"Invalid company details for symbol: {stock_symbol}")
        return None

    try:
        parts = data.split("|")
        if len(parts) < 4:
            logger.error(f"Unexpected message format: {data}")
            return None

        recv_data = parts[3]
        result = recv_data.split("^")
        if len(result) > 12:
            # 한국 시간 문자열 생성
            korea_time = datetime.now(ZoneInfo("Asia/Seoul"))
            korea_time_str = korea_time.strftime("%Y-%m-%d %H:%M:%S")
            return {
                "id": stock_info["id"],
                "name": stock_info["name"],
                "symbol": stock_symbol,
                "timestamp": korea_time_str,  # 한국 시간 보정
                "open": result[7],
                "close": result[2],
                "high": result[8],
                "low": result[9],
                "rate_price": result[4],
                "rate": result[5],
                "volume": result[12],
                "trading_value": float(result[2]) * int(result[12]),
            }
        else:
            logger.error(f"Unexpected result format for data: {result}")
    except (IndexError, ValueError, TypeError) as e:
        logger.error(f"Error processing stock data for Kafka: {data}. Error: {e}")
    return None


async def handle_message(data_queue: asyncio.Queue, message: str):
    try:
        # 메시지를 `|`로 나누고 심볼 추출
        parts = message.split("|")
        if len(parts) < 4:
            logger.error(f"Invalid message format: {message}")
            return

        stock_symbol = parts[3].split("^")[0]  # 세부 데이터의 첫 번째 항목이 심볼
        kafka_data = process_data_for_kafka(message, stock_symbol)
        if kafka_data:
            if data_queue.qsize() > 1000:  # 큐 크기 제한
                logger.warning("Data queue size exceeded limit. Dropping oldest data.")
                await data_queue.get()
            await data_queue.put(kafka_data)
    except Exception as e:
        logger.error(f"Error processing message: {message}. Error: {e}")


# WebSocket 핸들러 함수
async def websocket_handler(stock_symbols: List[Dict[str, str]], data_queue: asyncio.Queue):
    approval_key = await get_approval()
    if not approval_key:
        logger.error("Approval key not obtained. Terminating connection.")
        return

    url = "ws://ops.koreainvestment.com:31000"
    retry_count = 0
    max_retries = 10

    while retry_count < max_retries:
        try:
            async with websockets.connect(url, ping_interval=30) as websocket:
                for stock in stock_symbols:
                    await subscribe(websocket, approval_key, stock["symbol"])

                async for message in websocket:
                    asyncio.create_task(handle_message(data_queue, message))
            retry_count = 0  # 성공 시 재시도 횟수 초기화
        except websockets.ConnectionClosed as e:
            retry_count += 1
            logger.warning(f"WebSocket connection closed: {e}. Retrying in {2 ** retry_count} seconds...")
            await asyncio.sleep(2 ** retry_count)
        except Exception as e:
            retry_count += 1
            logger.error(f"Error in WebSocket handler: {e}. Retrying in {2 ** retry_count} seconds...")
            await asyncio.sleep(2 ** retry_count)
    logger.error("Max retries reached. WebSocket handler exiting.")


# Kafka 데이터 전송 태스크
async def kafka_producer_task(data_queue: asyncio.Queue, producer):
    while True:
        data = await data_queue.get()
        if data is None:  # 종료 신호
            break
        try:
            # 세부 키 생성
            key = generate_key(data["symbol"], data["timestamp"])
            await producer.send_and_wait(TOPIC_STOCK_DATA, key=key, value=data)
            logger.info(f"Sent data to Kafka: {data}")
        except Exception as e:
            logger.error(f"Failed to send data to Kafka: {e}")
        finally:
            data_queue.task_done()


# WebSocket 실행 함수
async def run_websocket_background_multiple(stock_symbols: List[Dict[str, str]]):
    data_queue = asyncio.Queue(maxsize=1000)
    producer = await init_kafka_producer()
    if not producer:
        logger.error("Kafka producer initialization failed.")
        return
    try:
        # Kafka 프로듀서와 WebSocket 핸들러 비동기 실행
        asyncio.create_task(kafka_producer_task(data_queue, producer))
        await websocket_handler(stock_symbols, data_queue)
    finally:
        # Kafka Producer 안전 종료
        await close_kafka_producer(producer)






async def generate_mock_stock_message(symbol):
    """Simulates real-time stock message generation."""
    parts = [
        "SOME_HEADER",
        "SOME_OTHER_PART",
        "STOCK_DATA",
        f"{symbol}^12345^1000^{random.uniform(50.0, 60.0):.2f}^"
        f"{random.uniform(51.0, 61.0):.2f}^{random.uniform(-2.0, 2.0):.2f}^"
        f"{random.randint(500, 1500)}^{random.uniform(51.2, 61.2):.2f}^"
        f"{random.uniform(50.0, 55.0):.2f}^50.0^100^{random.randint(1000, 10000)}^"
        f"{random.randint(100000, 500000)}",
    ]
    return "|".join(parts)


async def websocket_handler_mock(stock_symbols, data_queue):
    """Simulates an infinite stream of WebSocket messages."""
    try:
        while True:  # 지속적으로 데이터를 생성
            for stock in stock_symbols:
                message = await generate_mock_stock_message(stock["symbol"])
                await handle_message(data_queue, message)
            await asyncio.sleep(0.5)  # 0.5초 간격으로 데이터 생성
    except Exception as e:
        logger.error(f"Error in mock WebSocket handler: {e}")


async def kafka_producer_task_mock(data_queue, producer):
    """Simulates a continuous Kafka producer."""
    while True:
        try:
            # 데이터 가져오기
            data = await data_queue.get()
            if data is None:  # 종료 신호 처리
                break

            # 세부 키 생성
            key = generate_key(data["symbol"], data["timestamp"])
            await producer.send_and_wait(TOPIC_STOCK_DATA, key=key, value=data)
            # Kafka로 전송 시 key와 value를 로깅
            logger.info(f"Mock sent data to Kafka with key: {key}, value: {data}")
        except Exception as e:
            logger.error(f"Failed to mock send data to Kafka: {e}")
        finally:
            # 데이터 처리 완료
            data_queue.task_done()

# WebSocket과 Kafka 실행 함수
async def run_websocket_background_multiple_mock(stock_symbols):
    """Runs the continuous mock WebSocket and Kafka flow."""
    data_queue = asyncio.Queue(maxsize=1000)

    # Kafka Producer 초기화
    producer = await init_kafka_producer()
    if not producer:
        logger.error("Kafka producer initialization failed.")
        return

    try:
        # Kafka producer task 실행
        asyncio.create_task(kafka_producer_task_mock(data_queue, producer))

        # WebSocket handler 실행
        await websocket_handler_mock(stock_symbols, data_queue)
    finally:
        # Kafka Producer 안전 종료
        await close_kafka_producer(producer)