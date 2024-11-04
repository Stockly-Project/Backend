import json
import os
import websocket
import threading
import time
import random
import asyncio
from typing import Dict, List
from src.common.producer import send_to_kafka, init_kafka_producer
from src.logger import logger
import requests

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
TOPIC_STOCK_DATA = "real_time_stock_prices"

# Kafka Producer 초기화
producer = init_kafka_producer()

# 캐시된 승인 키
approval_key_cache = None

def get_approval(key, secret):
    global approval_key_cache
    if approval_key_cache:
        return approval_key_cache  # 캐시된 승인 키 사용

    url = 'https://openapivts.koreainvestment.com:29443'
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": key,
        "secretkey": secret
    }
    PATH = "oauth2/Approval"
    URL = f"{url}/{PATH}"

    res = requests.post(URL, headers=headers, data=json.dumps(body))
    approval_key = res.json().get("approval_key")
    approval_key_cache = approval_key  # 승인 키 캐시

    return approval_key

# 구독 메시지 생성 함수
def build_message(app_key, tr_id, tr_key, tr_type="1"):
    header = {
        "approval_key": app_key,
        "custtype": "P",
        "tr_type": tr_type,
        "content-type": "utf-8"
    }
    body = {"input": {"tr_id": tr_id, "tr_key": tr_key}}
    return json.dumps({"header": header, "body": body})

# 종목 구독 함수
def subscribe(ws, tr_id, app_key, stock_code):
    message = build_message(app_key, tr_id, stock_code)
    ws.send(message)
    time.sleep(1)  # 지연 시간 증가

# WebSocket 연결 후 다중 종목 구독 설정
def on_open(ws, stock_symbols):
    approval_key = get_approval(APP_KEY, APP_SECRET)
    for stock in stock_symbols:
        stock_code = stock["symbol"]
        subscribe(ws, "H0STASP0", approval_key, stock_code)  # 실시간 호가 구독
        subscribe(ws, "H0STCNT0", approval_key, stock_code)  # 실시간 체결 구독
        logger.debug(f"Subscribed to BID_ASK and CONTRACT for {stock_code}")

# WebSocket 에러 및 종료 핸들러
def on_error(ws, error):
    logger.error(f'WebSocket error occurred: {error}')
    if isinstance(error, OSError) and error.errno == 32:
        logger.error("Broken pipe error detected. Connection might be closed unexpectedly.")

def on_close(ws, status_code, close_msg):
    logger.info(f'WebSocket closed with status code={status_code}, message={close_msg}')

# Kafka로 전송할 주식 데이터 처리 함수
def process_data_for_kafka(data, stock_symbol):
    try:
        d1 = data.split("|")
        if len(d1) >= 4:
            recvData = d1[3]
            result = recvData.split("^")

            if len(result) > 12:
                stock_data = {
                    "symbol": stock_symbol,
                    "date": result[1],
                    "open": result[7],
                    "close": result[2],
                    "high": result[8],
                    "low": result[9],
                    "rate_price": result[4],
                    "rate": result[5],
                    "volume": result[12],
                }
                return stock_data
            else:
                logger.error(f"Unexpected result format for data: {result}")
    except (IndexError, ValueError, TypeError) as e:
        logger.error(f"Error processing stock data for Kafka: {e}")
    return None

# SSE로 전송할 주식 데이터 처리 함수
def process_data_for_sse(data, stock_info):
    try:
        if not isinstance(stock_info, dict):
            logger.error(f"Expected `stock_info` as dict, but got {type(stock_info)} with value {stock_info}")
            return None

        d1 = data.split("|")
        if len(d1) >= 4:
            recvData = d1[3]
            result = recvData.split("^")

            if len(result) > 12:
                close = float(result[2])
                rate_price = float(result[4])
                rate = float(result[5])
                volume = int(result[12])

                stock_data = {
                    "id": int(stock_info.get("id", 0)),
                    "name": stock_info.get("name", ""),
                    "symbol": stock_info.get("symbol", ""),
                    "close": close,
                    "rate_price": rate_price,
                    "rate": rate,
                    "volume": volume,
                    "volume_price": volume * close
                }
                return stock_data
            else:
                logger.error(f"Unexpected result format for data: {result}")
    except (IndexError, ValueError, TypeError) as e:
        logger.error(f"Error processing stock data for SSE: {e}")
    return None

# SSE 이벤트 생성기
async def sse_event_generator(data_queue: asyncio.Queue):
    while True:
        data = await data_queue.get()
        yield f"data: {json.dumps(data)}\n\n"
        await asyncio.sleep(0.3)

# WebSocket 메시지 핸들러
def handle_message(ws, message, stock_symbols, data_queue):
    if message[0] in ['0', '1']:
        for stock_info in stock_symbols:
            stock_symbol = stock_info["symbol"]

            # Kafka로 전송할 데이터 처리
            kafka_data = process_data_for_kafka(message, stock_symbol)
            if kafka_data:
                send_to_kafka(producer, TOPIC_STOCK_DATA, kafka_data)

            # SSE로 전송할 데이터 처리
            sse_data = process_data_for_sse(message, stock_info)
            if sse_data:
                logger.debug(f"SSE Data: {sse_data}")
                data_queue.put_nowait(sse_data)
    else:
        logger.info(f"Received non-stock message: {message[:100]}")

# WebSocket 연결 설정 및 스레드 실행
def websocket_thread(stock_symbols, data_queue):
    logger.info("Starting WebSocket thread for symbols: %s", stock_symbols)

    def on_open_wrapper(ws):
        on_open(ws, stock_symbols)

    while True:
        try:
            ws = websocket.WebSocketApp(
                "ws://ops.koreainvestment.com:31000",
                on_open=on_open_wrapper,
                on_message=lambda ws, message: handle_message(ws, message, stock_symbols, data_queue),
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=30)  # Ping 간격을 30초로 줄임
            logger.info("WebSocket thread has been terminated.")

        except Exception as e:
            logger.error(f"WebSocket error occurred: {e}")
            time.sleep(5)  # 재연결 시도 전 5초 대기
            logger.info("Attempting to reconnect WebSocket...")

# WebSocket 백그라운드 실행 함수
async def run_websocket_background_multiple(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
    data_queue = asyncio.Queue()
    ws_thread = threading.Thread(target=websocket_thread, args=(stock_symbols, data_queue))
    ws_thread.start()
    return data_queue











# Mock 데이터 생성 - 다중 회사
def generate_mock_multiple_stock_data(stock_info):
    stock_data = {
        "symbol": stock_info["symbol"],
        "date": "20241031",
        "open": str(random.uniform(50000, 55000)),
        "close": str(random.uniform(50000, 55000)),
        "high": str(random.uniform(55000, 60000)),
        "low": str(random.uniform(50000, 51000)),
        "rate_price": str(random.uniform(-5, 5)),
        "rate": str(random.uniform(-2, 2)),
        "volume": str(random.randint(1000, 5000)),
    }
    return stock_data

# Mock 데이터 전송 - 다중 회사
def mock_websocket_multiple_companies(stock_symbols):
    while True:
        for stock_info in stock_symbols:
            stock_data = generate_mock_multiple_stock_data(stock_info)
            send_to_kafka(producer, TOPIC_STOCK_DATA, stock_data)
            logger.debug(f"Sent mock data to Kafka for company: {stock_data}")
        time.sleep(1)  # 1초 대기 후 다음 데이터 생성

# WebSocket 백그라운드 실행 - 다중 회사 (Mock)
def run_mock_websocket_background_multiple(stock_symbols):
    ws_thread = threading.Thread(target=mock_websocket_multiple_companies, args=(stock_symbols,))
    ws_thread.start()
    return ws_thread


# Mock 데이터 생성 - 다중 회사
def generate_mock_multiple_stock_data(stock_info):
    stock_data = {
        "symbol": stock_info["symbol"],
        "date": "20241031",
        "open": str(random.uniform(50000, 55000)),
        "close": str(random.uniform(50000, 55000)),
        "high": str(random.uniform(55000, 60000)),
        "low": str(random.uniform(50000, 51000)),
        "rate_price": str(random.uniform(-5, 5)),
        "rate": str(random.uniform(-2, 2)),
        "volume": str(random.randint(1000, 5000)),
    }
    return stock_data

# Mock 데이터 전송 - 다중 회사
async def mock_websocket_multiple_companies(stock_symbols, producer):
    while True:
        batch_data = []
        for stock_info in stock_symbols:
            stock_data = generate_mock_multiple_stock_data(stock_info)
            batch_data.append(stock_data)
            send_to_kafka(producer, TOPIC_STOCK_DATA, stock_data)
            logger.debug(f"Sent mock data to Kafka for company: {stock_data}")
        await asyncio.sleep(1)  # 1초 대기 후 다음 데이터 생성


# WebSocket 백그라운드 실행 - 다중 회사 (Mock)
async def run_mock_websocket_background_multiple(stock_symbols):
    await mock_websocket_multiple_companies(stock_symbols, producer)


# Mock 데이터 생성 - 다중 회사
def generate_mock_multiple_stock_data(stock_info: Dict[str, str]):
    stock_data = {
        "id": stock_info["id"],
        "name": stock_info["name"],
        "symbol": stock_info["symbol"],
        "close": str(random.uniform(50000, 55000)),
        "rate_price": str(random.uniform(-5, 5)),
        "rate": str(random.uniform(-2, 2)),
        "volume": str(random.randint(1000, 5000)),
        "volume_price": str(random.uniform(5000000, 10000000)),
    }
    return stock_data

# Mock 데이터 전송 - 다중 회사
async def mock_websocket_multiple_companies(stock_symbols, data_queue):
    while True:
        batch_data = []  # 20개 회사 데이터를 모을 리스트
        for stock_info in stock_symbols:
            stock_data = generate_mock_multiple_stock_data(stock_info)
            batch_data.append(stock_data)
            logger.debug(f"Generated mock data for company {stock_info['symbol']}: {stock_data}")

        await data_queue.put(batch_data)
        logger.debug(f"Queued batch mock data for multiple companies: {batch_data}")
        await asyncio.sleep(1)  # 1초 대기


# WebSocket 백그라운드 실행 - 다중 회사 (Mock)
async def run_mock_websocket_background_multiple(stock_symbols):
    data_queue = asyncio.Queue()
    await asyncio.create_task(mock_websocket_multiple_companies(stock_symbols, data_queue))
    return data_queue
