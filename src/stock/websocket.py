import os
import asyncio
from src.common.producer import send_to_kafka, init_kafka_producer
from src.logger import logger
from .crud import get_company_details
from datetime import datetime
import websockets
import aiohttp
from typing import List, Dict
import json
import random 

TOPIC_STOCK_DATA = "real_time_stock_prices"

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
                "tr_id": "H0STCNT0",
                "tr_key": stock_code
            }
        }
    })
    await websocket.send(message)
    logger.info(f"Subscribed to H0STCNT0 for stock: {stock_code}")

# Kafka로 전송할 주식 데이터 처리 함수
def process_data_for_kafka(data, stock_symbol):
    stock_info = get_company_details(stock_symbol)
    # 반환 데이터 검증
    if not isinstance(stock_info, dict) or "id" not in stock_info or "name" not in stock_info:
        logger.error(f"Invalid company details for symbol: {stock_symbol}")
        return None
    
    id = stock_info.get("id")
    name = stock_info.get("name")
    if not stock_info or "id" not in stock_info or "name" not in stock_info:
        logger.error(f"No valid company information for symbol: {stock_symbol}")
        return None

    try:
        d1 = data.split("|")
        if len(d1) >= 4:
            recvData = d1[3]
            result = recvData.split("^")
            if len(result) > 12:
                current_date = datetime.now().strftime("%Y-%m-%d")
                api_time = result[1]  # "HHMMSS" 형식
                full_datetime = datetime.strptime(f"{current_date} {api_time}", "%Y-%m-%d %H%M%S")
                formatted_datetime = full_datetime.strftime("%Y-%m-%d %H:%M:%S")

                stock_data = {
                    "id": id,
                    "name": name,
                    "symbol": stock_symbol,
                    "date": formatted_datetime,
                    "open": result[7],
                    "close": result[2],
                    "high": result[8],
                    "low": result[9],
                    "rate_price": result[4],
                    "rate": result[5],
                    "volume": result[12],
                    "trading_value": float(result[2]) * int(result[12]),
                    "timestamp": full_datetime.timestamp(),  # 타임스탬프 추가
                }
                if not isinstance(stock_data, dict):
                    raise ValueError("Processed data is not a dictionary")
                return stock_data
            else:
                logger.error(f"Unexpected result format for data: {result}")
    except (IndexError, ValueError, TypeError) as e:
        logger.error(f"Error processing stock data for Kafka: {e}")
    return None

# WebSocket 메시지 처리 함수 (비동기 작업으로 메시지를 큐에 추가)
async def handle_message(data_queue: asyncio.Queue, message: str, stock_symbol: str):
    kafka_data = process_data_for_kafka(message, stock_symbol)
    if kafka_data:
        # 데이터 형식을 로깅하여 확인
        logger.debug(f"Data added to queue: {type(kafka_data)}, content: {kafka_data}")
        if isinstance(kafka_data, dict):  # JSON 직렬화 가능한 형식인지 확인
            await data_queue.put(kafka_data)
        else:
            logger.error(f"Invalid data format added to queue: {type(kafka_data)}")

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
async def kafka_producer_task(data_queue: asyncio.Queue, producer, topic="default_topic"):
    while True:
        data = await data_queue.get()
        if data is None:  # 종료 신호
            break

        try:
            # 데이터가 이미 직렬화된 상태인지 확인
            if isinstance(data, str):  # 이미 JSON 문자열인 경우
                serialized_data = data.encode('utf-8')
            elif isinstance(data, dict):  # 딕셔너리인 경우 직렬화
                serialized_data = json.dumps(data).encode('utf-8')
            else:
                raise TypeError(f"Unexpected data format: {type(data)}")

            # Kafka로 데이터 전송
            await producer.send_and_wait(topic, value=serialized_data)
            logger.debug(f"Data to be sent to Kafka: {data}, Type: {type(data)}")
            logger.info(f"Sent data to Kafka for symbol: {data.get('symbol', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to send data to Kafka: {e}")
        finally:
            data_queue.task_done()




# WebSocket 연결을 비동기적으로 실행
async def run_websocket_background_multiple(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
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









# 모의 주식 데이터 생성 함수
def generate_mock_stock_data(stock_symbol: str) -> Dict:
    stock_info = get_company_details(stock_symbol)
    id = stock_info.get("id", random.randint(1000, 9999))
    name = stock_info.get("name", f"Mock Company {stock_symbol}")
    current_date = datetime.now().strftime("%Y-%m-%d")
    current_time = datetime.now().strftime("%H%M%S")
    full_datetime = datetime.strptime(f"{current_date} {current_time}", "%Y-%m-%d %H%M%S")
    formatted_datetime = full_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    stock_data = {
        "id": id,
        "name": name,
        "symbol": stock_symbol,
        "date": formatted_datetime,
        "open": random.uniform(50000, 55000),
        "close": random.uniform(50000, 55000),
        "high": random.uniform(55000, 60000),
        "low": random.uniform(50000, 51000),
        "rate_price": random.uniform(-5, 5),
        "rate": random.uniform(-2, 2),
        "volume": random.randint(1000, 5000),
        "trading_value": random.uniform(50, 51),
        "timestamp": full_datetime.timestamp(),
    }
    return stock_data

# 모의 WebSocket 핸들러 함수 (실제 WebSocket 연결 대신 주기적으로 데이터를 생성)
async def mock_websocket_handler(stock_symbols: List[Dict[str, str]], data_queue: asyncio.Queue):
    while True:
        for stock in stock_symbols:
            stock_symbol = stock["symbol"]
            mock_data = generate_mock_stock_data(stock_symbol)
            await data_queue.put(mock_data)  # 모의 데이터를 비동기 큐에 추가
            logger.info(f"Generated mock data for symbol: {stock_symbol}")
        await asyncio.sleep(random.uniform(0.3, 0.7))  # 1초에 2-3번씩 데이터를 생성


# WebSocket 연결을 비동기적으로 실행 (mock 데이터 기반)
async def run_mock_websocket_background(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
    data_queue = asyncio.Queue()
    
    # Kafka Producer 비동기 초기화
    producer = await init_kafka_producer()
    if producer is None:
        logger.error("Kafka producer initialization failed. Exiting.")
        return data_queue

    # Kafka 전송 작업 비동기 시작
    asyncio.create_task(kafka_producer_task(data_queue, producer))
    
    # 모의 WebSocket 데이터 생성 핸들러 비동기 시작
    await mock_websocket_handler(stock_symbols, data_queue)

    return data_queue
