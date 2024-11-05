import json
import os
import websocket
import threading
import time
import asyncio
from typing import Dict, List
from src.common.producer import send_to_kafka, init_kafka_producer
from src.logger import logger
import requests
import random
from .crud import get_company_details
from datetime import datetime

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
TOPIC_STOCK_DATA = "real_time_stock_prices"

# Kafka Producer 초기화
producer = init_kafka_producer()

def get_approval(app_key, app_secret):
    url = 'https://openapivts.koreainvestment.com:29443/oauth2/Approval'
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "secretkey": app_secret
    }

    response = requests.post(url, headers=headers, data=json.dumps(body))
    if response.status_code == 200 and "approval_key" in response.json():
        approval_key = response.json()["approval_key"]
        return approval_key
    else:
        logger.error(f"Failed to get approval key: {response.text}")
        return None

def build_message(app_key, tr_id, tr_key, tr_type="1"):
    header = {
        "approval_key": app_key,
        "app_key": APP_KEY,
        "secret_key": APP_SECRET,
        "custtype": "P",
        "tr_type": tr_type,
        "content-type": "utf-8"
    }
    body = {"input": {"tr_id": tr_id, "tr_key": tr_key}}
    return json.dumps({"header": header, "body": body})

# 구독 함수
def subscribe(ws, tr_id, app_key, stock_code):
    message = build_message(app_key, tr_id, stock_code)
    ws.send(message)
    time.sleep(4.0)

# WebSocket 연결 후 다중 종목 구독 설정
def on_open(ws, stock_symbols):
    approval_key = get_approval(APP_KEY, APP_SECRET)
    if not approval_key:
        logger.error("Approval key not obtained, terminating connection.")
        ws.close()
        return

    for stock in stock_symbols:
        stock_code = stock["symbol"]
        # subscribe(ws, "H0STASP0", approval_key, stock_code)
        subscribe(ws, "H0STCNT0", approval_key, stock_code)
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
    stock_info = get_company_details(stock_symbol)  # 데이터베이스에서 회사 정보 조회
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
                stock_data = {
                    "id": id,
                    "name": name,
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


def handle_message(ws, message, stock_symbols, data_queue):
    if message.startswith("{"):
        # JSON 메시지 처리
        try:
            message_data = json.loads(message)
            tr_id = message_data.get("header", {}).get("tr_id")
            if tr_id in ["H0STCNT0"] and message_data.get("body", {}).get("rt_cd") == "1":
                logger.info(f"Subscription confirmation for {tr_id} - {message_data}")
                return
        except json.JSONDecodeError:
            logger.error(f"Failed to parse message as JSON: {message[:100]}")
    else:
        # 구분자 형식 데이터 처리
        d1 = message.split("|")
        if len(d1) >= 3:
            tr_id, stock_symbol = d1[1], d1[3].split("^")[0]
            logger.debug(f"Processing data for tr_id: {tr_id}, stock_symbol: {stock_symbol}")

            kafka_data = process_data_for_kafka(message, stock_symbol)
            if kafka_data:
                send_to_kafka(producer, TOPIC_STOCK_DATA, json.dumps(kafka_data))


# WebSocket 연결 설정 및 스레드 실행
def websocket_thread(stock_symbols, data_queue):
        try:
            ws = websocket.WebSocketApp(
                "ws://ops.koreainvestment.com:31000",
                on_open=lambda ws: on_open(ws, stock_symbols),
                on_message=lambda ws, message: handle_message(ws, message, stock_symbols, data_queue),
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=60)
            logger.info("WebSocket thread has been terminated.")
        except Exception as e:
            logger.error(f"WebSocket error occurred: {e}")
            time.sleep(0.3)
            logger.info("Attempting to reconnect WebSocket...")

# WebSocket 백그라운드 실행 함수
async def run_websocket_background_multiple(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
    data_queue = asyncio.Queue()
    ws_thread = threading.Thread(target=websocket_thread, args=(stock_symbols, data_queue))
    ws_thread.start()
    return data_queue







# Mock 데이터 생성 함수 - 개별 주식 데이터 생성
def generate_single_mock_stock_data(stock_info: Dict[str, str]) -> Dict[str, str]:
    # 주식 정보 조회 및 목업 데이터 생성
    stock = get_company_details(stock_info["symbol"])  # 데이터베이스에서 회사 정보 조회
    if stock:
        id = stock.get("id")
        name = stock.get("name")
    else:
        id, name = None, None  # 기본값으로 설정

    return {
        "id": id,
        "name": name,
        "symbol": stock_info["symbol"],
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "open": str(random.uniform(50000, 55000)),
        "close": str(random.uniform(50000, 55000)),
        "high": str(random.uniform(55000, 60000)),
        "low": str(random.uniform(50000, 51000)),
        "rate_price": str(random.uniform(-5, 5)),
        "rate": str(random.uniform(-2, 2)),
        "volume": str(random.randint(1000, 5000)),
    }

# Mock WebSocket 데이터 생성 및 Queue에 전송
async def run_mock_websocket_background_multiple(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
    data_queue = asyncio.Queue()

    async def mock_data_producer():
        while True:
            for stock_info in stock_symbols:
                mock_data = generate_single_mock_stock_data(stock_info)
                await data_queue.put(json.dumps(mock_data))  # Queue에 JSON 문자열 형태로 데이터 넣기
                send_to_kafka(producer, TOPIC_STOCK_DATA, json.dumps(mock_data))  # Kafka로 전송
            await asyncio.sleep(0.5)

    asyncio.create_task(mock_data_producer())
    return data_queue

# Mock 데이터 SSE 이벤트 생성기
async def sse_mock_event_generator(stock_symbols: List[Dict[str, str]]):
    while True:
        for stock_info in stock_symbols:
            mock_data = generate_single_mock_stock_data(stock_info)
            yield f"data: {json.dumps(mock_data)}\n\n"
        await asyncio.sleep(1)