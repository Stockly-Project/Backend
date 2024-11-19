import os
import websockets
import aiohttp
import asyncio
from typing import Dict, List
from src.common.producer import init_kafka_producer
from src.logger import logger
from .crud import get_company_details
import orjson

TOPIC_STOCK_DATA = "real_time_asking_prices"


# 승인 키 가져오는 함수
async def get_approval():
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


# WebSocket 메시지 처리 함수
async def handle_message(data_queue: asyncio.Queue, message: str, stock_symbol: str):
    kafka_data = process_data_for_kafka(message, stock_symbol)
    if kafka_data:
        try:
            if data_queue.qsize() > 1000:  # 큐 크기 제한
                logger.warning("Data queue size exceeded limit. Dropping oldest data.")
                await data_queue.get()
            await data_queue.put(kafka_data)
        except Exception as e:
            logger.error(f"Failed to add data to queue: {e}")


# WebSocket 핸들러 함수
async def websocket_handler(stock_symbols: List[Dict[str, str]], data_queue: asyncio.Queue):
    approval_key = await get_approval()
    if not approval_key:
        logger.error("Approval key not obtained. Terminating WebSocket handler.")
        return

    url = "ws://ops.koreainvestment.com:31000"
    while True:
        try:
            async with websockets.connect(url, ping_interval=30) as websocket:
                # 병렬 구독 요청
                await asyncio.gather(*[subscribe(websocket, approval_key, stock["symbol"]) for stock in stock_symbols])

                # 메시지 수신 및 처리
                async for message in websocket:
                    d1 = message.split("|")
                    if len(d1) >= 3:
                        stock_symbol = d1[3].split("^")[0]
                        await handle_message(data_queue, message, stock_symbol)
        except websockets.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Error in WebSocket handler: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)


# Kafka 프로듀서 태스크
async def kafka_producer_task(data_queue: asyncio.Queue, producer, topic="real_time_asking_prices"):
    while True:
        data = await data_queue.get()
        if data is None:
            break

        try:
            if isinstance(data, dict):
                serialized_data = orjson.dumps(data)
                await producer.send_and_wait(topic, value=serialized_data)
                logger.info(f"Sent data to Kafka for symbol: {data.get('symbol', 'unknown')}")
            else:
                raise TypeError(f"Unexpected data format: {type(data)}")
        except Exception as e:
            logger.error(f"Failed to send data to Kafka: {e}")
        finally:
            data_queue.task_done()


# WebSocket 실행 함수
async def run_asking_websocket_background_multiple(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
    data_queue = asyncio.Queue(maxsize=1000)

    producer = await init_kafka_producer()
    if not producer:
        logger.error("Kafka producer initialization failed. Exiting WebSocket task.")
        return data_queue

    asyncio.create_task(kafka_producer_task(data_queue, producer))
    await websocket_handler(stock_symbols, data_queue)

    return data_queue
