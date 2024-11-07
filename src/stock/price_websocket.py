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
from .crud import get_company_details
import random

TOPIC_STOCK_DATA = "real_time_asking_prices"

# Kafka Producer 초기화
producer = init_kafka_producer()

def get_approval():
    url = 'https://openapivts.koreainvestment.com:29443/'
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials",
            "appkey": os.getenv("HOGA_KEY"),
            "secretkey": os.getenv("HOGA_SECRET")}
    PATH = "oauth2/Approval"
    URL = f"{url}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body))
    approval_key = res.json()["approval_key"]
    return approval_key

def build_message(app_key, tr_key):
    header = {
        "approval_key": app_key,
        "custtype": "P",
        "tr_type": "1",
        "content-type": "utf-8",
    }
    body = {
        "input": {
            "tr_id": "H0STASP0",
            "tr_key": tr_key
        }
    }
    return json.dumps({"header": header, "body": body})

# 구독 함수
def subscribe(ws, app_key, stock_code):
    message = build_message(app_key, stock_code)
    if ws.sock and ws.sock.connected:
        ws.send(message)
        logger.info(f"Subscribed to H0STCNT0 for stock: {stock_code}")
    else:
        logger.error("WebSocket not connected, cannot subscribe.")
    time.sleep(0.1)

# WebSocket 연결 후 다중 종목 구독 설정
def on_open(ws, stock_symbols):
    approval_key = get_approval()
    if not approval_key:
        logger.error("Approval key not obtained, terminating connection.")
        ws.close()
        return

    time.sleep(3)

    for stock in stock_symbols:
        stock_code = stock["symbol"]
        try:
            if ws.sock and ws.sock.connected:
                subscribe(ws, approval_key, stock_code)
                logger.debug(f"Subscribed to BID_ASK and CONTRACT for {stock_code}")
            else:
                logger.error(f"WebSocket not fully connected for {stock_code}, skipping subscription.")
                break
        except Exception as e:
            logger.error(f"Subscription failed for {stock_code}: {e}")
            ws.close()
            return

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


def handle_message(ws, message, stock_symbols, data_queue):
    if message.startswith("{"):
        try:
            message_data = json.loads(message)
            tr_id = message_data.get("header", {}).get("tr_id")
            if tr_id == "H0STASP0" and message_data.get("body", {}).get("rt_cd") == "1":
                logger.info(f"Subscription confirmation for {tr_id} - {message_data}")
                return
        except json.JSONDecodeError:
            logger.error(f"Failed to parse message as JSON: {message[:100]}")
    else:
        d1 = message.split("|")
        if len(d1) >= 3:
            tr_id, stock_symbol = d1[1], d1[3].split("^")[0]
            logger.debug(f"Processing data for tr_id: {tr_id}, stock_symbol: {stock_symbol}")

            kafka_data = process_data_for_kafka(message, stock_symbol)
            if kafka_data:
                send_to_kafka(producer, TOPIC_STOCK_DATA, json.dumps(kafka_data))

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
            ws.run_forever(ping_interval=30)
            logger.info("WebSocket thread has been terminated.")
        except Exception as e:
            logger.error(f"WebSocket error occurred: {e}")
            if isinstance(e, OSError) and e.errno == 32:
                logger.info("Re-obtaining approval key and attempting to reconnect...")
                _connect_key = get_approval()
            time.sleep(0.3)
            logger.info("Attempting to reconnect WebSocket...")

# WebSocket 백그라운드 실행 함수
async def run_asking_websocket_background_multiple(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
    data_queue = asyncio.Queue()
    ws_thread = threading.Thread(target=websocket_thread, args=(stock_symbols, data_queue))
    ws_thread.start()
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