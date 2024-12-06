import os
import websockets
import aiohttp
import asyncio
from typing import Dict, List
from src.common.producer import init_kafka_producer, close_kafka_producer
from src.logger import logger
from .crud import get_company_details
import orjson
import hashlib
import random
from datetime import datetime


TOPIC_STOCK_DATA = "real_time_asking_prices"


# 승인 키 캐싱 변수
approval_key_cache = None

async def get_approval():
    global approval_key_cache
    if approval_key_cache:  # 캐시된 키 반환
        return approval_key_cache

    url = 'https://openapivts.koreainvestment.com:29443/oauth2/Approval'
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": os.getenv("HOGA_KEY"),
        "secretkey": os.getenv("HOGA_SECRET")
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


async def subscribe(websocket, app_key, stock_code):
    message = orjson.dumps({
        "header": {
            "approval_key": app_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8",
        },
        "body": {
            "input": {
                "tr_id": "H0STASP0",
                "tr_key": stock_code
            }
        }
    }).decode("utf-8")
    await websocket.send(message)
    logger.info(f"Subscribed to H0STASP0 for stock: {stock_code}")


def process_data_for_kafka(data, stock_symbol):
    try:
        parts = data.split("|")
        if len(parts) < 4:
            logger.error(f"Invalid data format (expected at least 4 parts): {data}")
            return None

        recv_data = parts[3]
        result = recv_data.split("^")
        if len(result) <= 42:  # 최소 필요한 데이터 길이 확인
            logger.error(f"Unexpected result format (length <= 42): {result}")
            return None

        # 파싱된 데이터를 Dict로 구성
        sell_prices = {f"sell_price_{i + 3}": result[12 - i] for i in range(8)}
        sell_volumes = {f"sell_volume_{i + 3}": result[32 - i] for i in range(8)}
        buy_prices = {f"buy_price_{i + 1}": result[13 + i] for i in range(8)}
        buy_volumes = {f"buy_volume_{i + 1}": result[33 + i] for i in range(8)}

        stock_data = {
            "symbol": stock_symbol,
            **sell_prices,
            **sell_volumes,
            **buy_prices,
            **buy_volumes,
        }
        logger.debug(f"Processed data for Kafka: {stock_data}")
        return stock_data

    except Exception as e:
        logger.error(f"Error processing stock data for Kafka: {e}, Data: {data}")
        return None


async def handle_message(data_queue: asyncio.Queue, message: str):
    try:
        parts = message.split("|")
        if len(parts) < 4:
            logger.error(f"Invalid message format: {message}")
            return

        recv_data = parts[3]
        stock_symbol = recv_data.split("^")[0]

        kafka_data = process_data_for_kafka(message, stock_symbol)
        if not kafka_data:
            logger.warning(f"Processed data is invalid: {message}")
            return

        if data_queue.qsize() > 1000:  # 큐 크기 제한
            logger.warning("Data queue size exceeded limit. Dropping oldest data.")
            await data_queue.get()

        await data_queue.put(kafka_data)
        logger.info(f"Data added to queue: {kafka_data['symbol']}")
    except Exception as e:
        logger.error(f"Failed to handle WebSocket message: {e}, Message: {message}")


async def websocket_handler(stock_symbols: List[Dict[str, str]], data_queue: asyncio.Queue):
    approval_key = await get_approval()
    if not approval_key:
        logger.error("Approval key not obtained. Terminating WebSocket handler.")
        return

    url = "ws://ops.koreainvestment.com:31000"
    while True:
        try:
            async with websockets.connect(url, ping_interval=30) as websocket:
                await asyncio.gather(*[subscribe(websocket, approval_key, stock["symbol"]) for stock in stock_symbols])

                async for message in websocket:
                    logger.debug(f"Received WebSocket message: {message}")
                    await handle_message(data_queue, message)
        except websockets.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Error in WebSocket handler: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)


async def kafka_producer_task(data_queue: asyncio.Queue, producer, topic="real_time_asking_prices"):
    while True:
        data = await data_queue.get()
        if not isinstance(data, dict):
            logger.error(f"Invalid data for Kafka: {data}")
            continue

        try:
            serialized_data = orjson.dumps(data)
            await producer.send_and_wait(topic, value=serialized_data)
            logger.info(f"Sent data to Kafka: {data.get('symbol', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to send data to Kafka: {e}, Data: {data}")
        finally:
            data_queue.task_done()


async def run_asking_websocket_background_multiple(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
    data_queue = asyncio.Queue(maxsize=1000)
    producer = await init_kafka_producer()
    if not producer:
        logger.error("Kafka producer initialization failed. Exiting WebSocket task.")
        return data_queue

    try:
        asyncio.create_task(kafka_producer_task(data_queue, producer))
        await websocket_handler(stock_symbols, data_queue)
    finally:
        await close_kafka_producer(producer)

    return data_queue



def process_mock_data_for_kafka(message: Dict, stock_symbol: str) -> Dict:
    try:
        # 메시지가 dict인지 확인
        if not isinstance(message, dict):
            logger.error(f"Expected dict, but got {type(message)}")
            return {}

        # 필요한 데이터 추출
        required_keys = [
            "id", "symbol", "name", "sell_price_3", "sell_price_4", "sell_price_5",
            "sell_volume_3", "sell_volume_4", "sell_volume_5",
            "buy_price_1", "buy_price_2", "buy_price_3",
            "buy_volume_1", "buy_volume_2", "buy_volume_3", "timestamp"
        ]
        
        # 데이터 필터링
        filtered_data = {key: message[key] for key in required_keys if key in message}
        
        if len(filtered_data) != len(required_keys):
            missing_keys = set(required_keys) - set(filtered_data.keys())
            logger.warning(f"Missing keys in message: {missing_keys}")
            return {}

        # Kafka용 데이터 생성
        kafka_data = {
            "symbol": stock_symbol,
            "data": filtered_data
        }
        return kafka_data

    except Exception as e:
        logger.error(f"Error processing stock data for Kafka: {e}")
        return {}




async def handle_mock_message(data_queue: asyncio.Queue, message: Dict):
    try:
        # 메시지가 dict인지 확인
        if not isinstance(message, dict):
            logger.error(f"Invalid message format (not a dict): {message}")
            return

        # 필요한 필드 확인
        required_fields = {"symbol", "timestamp"}
        if not required_fields.issubset(message.keys()):
            logger.error(f"Missing required fields in message: {message}")
            return

        stock_symbol = message["symbol"]
        kafka_data = process_mock_data_for_kafka(message, stock_symbol)

        # Kafka 데이터 유효성 검증
        if not kafka_data:
            logger.warning(f"Processed data is invalid: {message}")
            return

        # 큐 크기 제한 초과 시 데이터 삭제
        if data_queue.qsize() > 1000:
            logger.warning("Data queue size exceeded limit. Dropping oldest data.")
            await data_queue.get()

        # 데이터 큐에 추가
        await data_queue.put(kafka_data)
        logger.info(f"Data added to queue: {kafka_data.get('symbol', 'unknown')}")
    except Exception as e:
        logger.error(f"Failed to handle WebSocket message: {e}, Message: {message}")


# Mock Kafka 프로듀서 태스크
async def kafka_producer_task_mock(data_queue: asyncio.Queue, producer, topic="real_time_asking_prices"):
    while True:
        data = await data_queue.get()
        if data is None:
            continue  # None 데이터를 무시하고 다음 데이터를 처리

        try:
            # key = f"{data['symbol']}_{random.randint(1, 1000)}"  # 간단한 키 생성
            serialized_data = orjson.dumps(data)
            logger.error(f"Preparing to send mock data to Kafka: {serialized_data}")
            await producer.send_and_wait(topic, value=serialized_data)
            logger.info(f"Sent mock data to Kafka for symbol: {data.get('symbol', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to send mock data to Kafka: {e}")
        finally:
            data_queue.task_done()

async def generate_mock_data(stock_symbol: str) -> Dict:
    """모의 데이터를 생성하여 반환하는 함수"""
    stock_info = get_company_details(stock_symbol)
    if not stock_info:
        logger.error(f"Invalid stock information for symbol: {stock_symbol}")
        return {}

    mock_data = {
        "id": stock_info["id"],
        "symbol": stock_symbol,
        "name": stock_info["name"],
        "sell_price_3": random.uniform(100, 200),
        "sell_price_4": random.uniform(200, 300),
        "sell_price_5": random.uniform(300, 400),
        "sell_volume_3": random.randint(100, 1000),
        "sell_volume_4": random.randint(100, 1000),
        "sell_volume_5": random.randint(100, 1000),
        "buy_price_1": random.uniform(50, 100),
        "buy_price_2": random.uniform(100, 150),
        "buy_price_3": random.uniform(150, 200),
        "buy_volume_1": random.randint(100, 1000),
        "buy_volume_2": random.randint(100, 1000),
        "buy_volume_3": random.randint(100, 1000),
        "timestamp": datetime.now().isoformat()
    }
    logger.info(f"Mock data generated for symbol: {stock_symbol}")
    return mock_data


# Mock WebSocket 메시지 처리 함수
async def websocket_handler_mock(stock_symbols: List[Dict[str, str]], data_queue: asyncio.Queue):
    """모의 WebSocket 메시지를 처리하는 핸들러"""
    try:
        while True:
            for stock in stock_symbols:
                stock_symbol = stock["symbol"]
                mock_data = await generate_mock_data(stock_symbol)
                if mock_data:  # 유효한 데이터만 처리
                    await handle_mock_message(data_queue, mock_data)  # 수정된 함수 호출
            await asyncio.sleep(1)  # 1초 간격으로 데이터 생성
    except Exception as e:
        logger.error(f"Error in mock WebSocket handler: {e}")


# Mock WebSocket 실행 함수
async def run_asking_websocket_background_multiple_mock(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
    """모의 WebSocket 데이터 생성 및 Kafka 전송 실행"""
    data_queue = asyncio.Queue(maxsize=1000)
    producer = await init_kafka_producer()
    if not producer:
        logger.error("Kafka producer initialization failed. Exiting mock WebSocket task.")
        return data_queue

    try:
        asyncio.create_task(kafka_producer_task_mock(data_queue, producer))
        await websocket_handler_mock(stock_symbols, data_queue)
    finally:
        await close_kafka_producer(producer)

    return data_queue
