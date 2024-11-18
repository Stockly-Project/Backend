import json
import os
import websockets
import aiohttp
import threading
import time
import asyncio
from typing import Dict, List
from src.common.producer import send_to_kafka, init_kafka_producer
from src.logger import logger
import requests
from .crud import get_company_details
import random

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
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=body) as res:
            data = await res.json()
            return data.get("approval_key")

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
    logger.info(f"Subscribed to H0STCNT0 for stock: {stock_code}")

# WebSocket 메시지 처리 함수 (비동기 작업으로 메시지를 큐에 추가)
async def handle_message(data_queue: asyncio.Queue, message: str, stock_symbol: str):
    kafka_data = process_data_for_kafka(message, stock_symbol)
    if kafka_data:
        await data_queue.put(kafka_data)

# WebSocket 핸들러 함수
async def websocket_handler(stock_symbols: List[Dict[str, str]], data_queue: asyncio.Queue):
    approval_key = await get_approval()
    if not approval_key:
        logger.error("Approval key not obtained, terminating connection.")
        return

    url = "ws://ops.koreainvestment.com:31000"
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


# Kafka에 데이터를 비동기로 전송하는 함수
async def kafka_producer_task(data_queue: asyncio.Queue, producer):
    while True:
        data = await data_queue.get()
        try:
            await send_to_kafka(producer, TOPIC_STOCK_DATA, json.dumps(data))
            logger.info(f"Sent data to Kafka for symbol: {data['symbol']}")
        except Exception as e:
            logger.error(f"Failed to send data to Kafka: {e}")
        finally:
            data_queue.task_done()


# WebSocket 에러 및 종료 핸들러
def on_error(ws, error):
    logger.error(f'WebSocket error occurred: {error}')
    if isinstance(error, OSError) and error.errno == 32:
        logger.error("Broken pipe error detected. Connection might be closed unexpectedly.")

def on_close(ws, status_code, close_msg):
    logger.info(f'WebSocket closed with status code={status_code}, message={close_msg}')

def process_data_for_kafka(data, stock_symbol):
    stock_info = get_company_details(stock_symbol)
    id = stock_info.get("id")
    name = stock_info.get("name")

    if not stock_info or "id" not in stock_info or "name" not in stock_info:
        logger.error(f"No valid company information for symbol: {stock_symbol}")
        return None

    try:
        # 데이터 파싱
        d1 = data.split("|")
        if len(d1) >= 4:
            recvData = d1[3]
            result = recvData.split("^")
            if len(result) > 42:  # 필수 호가 데이터가 포함된 최소 인덱스 확인

                # 매도 호가 및 잔량 (3~10위)
                sell_prices = {
                    f"sell_price_{i + 3}": result[12 - i] for i in range(8)
                }
                sell_volumes = {
                    f"sell_volume_{i + 3}": result[32 - i] for i in range(8)
                }

                # 매수 호가 및 잔량 (1~8위)
                buy_prices = {
                    f"buy_price_{i + 1}": result[13 + i] for i in range(8)
                }
                buy_volumes = {
                    f"buy_volume_{i + 1}": result[33 + i] for i in range(8)
                }

                # 필요한 정보 딕셔너리 생성
                stock_data = {
                    "id": id,
                    "symbol": stock_symbol,
                    "name": name,
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

# WebSocket 연결을 비동기적으로 실행
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





# 개별 주식 데이터 생성 함수
def generate_single_mock_stock_data(stock_info: Dict[str, str]) -> Dict[str, str]:
    # 주식 정보 조회 및 목업 데이터 생성
    stock = get_company_details(stock_info["symbol"])  # 데이터베이스에서 회사 정보 조회
    if stock:
        id = stock.get("id")
        name = stock.get("name")
    else:
        id, name = None, None  # 기본값으로 설정

    # 매도 호가 및 잔량 (1~7위)
    sell_prices = {f"sell_price_{i}": str(random.uniform(30000, 31000)) for i in range(7)}
    sell_volumes = {f"sell_volume_{i}": str(random.randint(100, 1000)) for i in range(7)}

    # 매수 호가 및 잔량 (1~7위)
    buy_prices = {f"buy_price_{i}": str(random.uniform(29000, 30000)) for i in range(7)}
    buy_volumes = {f"buy_volume_{i}": str(random.randint(100, 1000)) for i in range(7)}

    # 필요한 정보 딕셔너리 생성
    stock_data = {
        "id": id,
        "name": name,
        "symbol": stock_info["symbol"],
        **sell_prices,
        **sell_volumes,
        **buy_prices,
        **buy_volumes
    }

    return stock_data

async def run_mock_asking_websocket_background_multiple(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
    data_queue = asyncio.Queue()

    async def mock_data_producer():
        while True:
            for stock_info in stock_symbols:
                mock_data = generate_single_mock_stock_data(stock_info)
                await data_queue.put(json.dumps(mock_data))  # Queue에 JSON 문자열 형태로 데이터 넣기
                send_to_kafka(producer, TOPIC_STOCK_DATA, json.dumps(mock_data))  # Kafka로 전송
            await asyncio.sleep(1)

    asyncio.create_task(mock_data_producer())
    return data_queue