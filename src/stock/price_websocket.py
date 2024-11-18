import json
import os
import websockets
import aiohttp
import asyncio
from typing import Dict, List
from src.common.producer import send_to_kafka, init_kafka_producer
from src.logger import logger
from .crud import get_company_details

TOPIC_STOCK_DATA = "real_time_asking_prices"


# 승인 키 가져오는 함수
async def get_approval():
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
                approval_key = data.get("approval_key")
                if approval_key:
                    logger.info("Approval key obtained successfully.")
                    return approval_key
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
                "tr_id": "H0STASP0",
                "tr_key": stock_code
            }
        }
    })
    await websocket.send(message)
    logger.info(f"Subscribed to H0STASP0 for stock: {stock_code}")


# WebSocket 메시지 처리 함수
async def handle_message(data_queue: asyncio.Queue, message: str, stock_symbol: str):
    kafka_data = process_data_for_kafka(message, stock_symbol)
    if kafka_data:
        try:
            if data_queue.qsize() > 1000:  # 메모리 누수를 방지하기 위한 큐 크기 제한
                logger.warning("Data queue size exceeded limit. Dropping oldest data.")
                await data_queue.get()  # 가장 오래된 데이터를 삭제
            await data_queue.put(kafka_data)
        except Exception as e:
            logger.error(f"Failed to add data to queue: {e}")


# WebSocket 핸들러 함수
async def websocket_handler(stock_symbols: List[Dict[str, str]], data_queue: asyncio.Queue):
    approval_key = await get_approval()
    if not approval_key:
        logger.error("Approval key not obtained, terminating connection.")
        return

    url = "ws://ops.koreainvestment.com:31000"
    while True:  # 재시도 로직 추가
        try:
            async with websockets.connect(url, ping_interval=60) as websocket:
                # 각 종목에 대해 구독
                for stock in stock_symbols:
                    await subscribe(websocket, approval_key, stock["symbol"])

                # 메시지 수신 및 처리
                async for message in websocket:
                    d1 = message.split("|")
                    if len(d1) >= 3:
                        stock_symbol = d1[3].split("^")[0]
                        await handle_message(data_queue, message, stock_symbol)
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Error in WebSocket handler: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
            
            
async def kafka_producer_task(data_queue: asyncio.Queue, producer, topic="real_time_asking_prices"):
    while True:
        data = await data_queue.get()
        if data is None:  # 종료 신호
            break

        try:
            # 데이터를 producer.send_and_wait에 직접 전달 (직렬화는 value_serializer에서 처리)
            if isinstance(data, dict):
                await producer.send_and_wait(topic, value=data)
                logger.info(f"Sent data to Kafka for symbol: {data.get('symbol', 'unknown')}")
                logger.debug(f"Data: {data}")
            else:
                raise TypeError(f"Unexpected data format: {type(data)}")
        except Exception as e:
            logger.error(f"Failed to send data to Kafka: {e}")
        finally:
            data_queue.task_done()


# Kafka 데이터 처리 함수
def process_data_for_kafka(data, stock_symbol):
    stock_info = get_company_details(stock_symbol)
    if not stock_info or "id" not in stock_info or "name" not in stock_info:
        logger.error(f"No valid company information for symbol: {stock_symbol}")
        return None

    try:
        d1 = data.split("|")
        if len(d1) >= 4:
            recvData = d1[3]
            result = recvData.split("^")
            if len(result) > 42:  # 필수 호가 데이터가 포함된 최소 인덱스 확인
                # 데이터 파싱
                sell_prices = {f"sell_price_{i + 3}": result[12 - i] for i in range(8)}
                sell_volumes = {f"sell_volume_{i + 3}": result[32 - i] for i in range(8)}
                buy_prices = {f"buy_price_{i + 1}": result[13 + i] for i in range(8)}
                buy_volumes = {f"buy_volume_{i + 1}": result[33 + i] for i in range(8)}

                stock_data = {
                    "id": stock_info["id"],
                    "symbol": stock_symbol,
                    "name": stock_info["name"],
                    **sell_prices,
                    **sell_volumes,
                    **buy_prices,
                    **buy_volumes
                }
                return stock_data
            else:
                logger.error(f"Unexpected result format for data: {result}")
    except (IndexError, ValueError, TypeError) as e:
        logger.error(f"Error processing stock data for Kafka: {e}")
    return None


# WebSocket 실행 함수
async def run_asking_websocket_background_multiple(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
    data_queue = asyncio.Queue()

    # Kafka Producer 비동기 초기화
    producer = await init_kafka_producer()
    if producer is None:
        logger.error("Kafka producer initialization failed. Exiting.")
        return data_queue

    # Kafka 전송 작업 비동기 시작
    asyncio.create_task(kafka_producer_task(data_queue, producer))
    
    # WebSocket 연결 핸들러 비동기 시작
    await websocket_handler(stock_symbols, data_queue)

    return data_queue
