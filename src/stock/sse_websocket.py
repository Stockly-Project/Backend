import json
import os
import websocket
import threading
import time
import asyncio
from typing import Dict, List
from src.logger import logger
import requests
from .crud import get_company_details


TOPIC_STOCK_DATA = "real_time_stock_prices"

def get_approval():
    url = 'https://openapivts.koreainvestment.com:29443/'
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials",
            "appkey": os.getenv("SSE_KEY"),
            "secretkey": os.getenv("SSE_SECRET")}
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
            "tr_id": "H0STCNT0",
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



event_loop = asyncio.get_event_loop()

# SSE 전용 WebSocket 스레드 실행 함수
def sse_websocket_thread(stock_symbols, data_queue, loop):
    logger.info("Starting SSE WebSocket thread for symbols: %s", stock_symbols)

    def on_open_wrapper(ws):
        on_open(ws, stock_symbols)

    def handle_sse_message(ws, message):
        if message.startswith("{"):
            return
        else:
            d1 = message.split("|")
            if len(d1) >= 3:
                stock_symbol = d1[3].split("^")[0]
                stock_data = process_data_for_sse(message, stock_symbol)  # SSE용 데이터 처리
                if stock_data:
                    # 메인 스레드의 이벤트 루프를 사용하여 Queue에 추가
                    asyncio.run_coroutine_threadsafe(data_queue.put(json.dumps(stock_data)), loop)
                    print(f"Added data for symbol {stock_symbol} to data_queue.")

    while True:
        try:
            ws = websocket.WebSocketApp(
                "ws://ops.koreainvestment.com:31000",
                on_open=on_open_wrapper,
                on_message=lambda ws, message: handle_sse_message(ws, message),
                on_error=on_error,
                on_close=on_close
            )
            ws.run_forever(ping_interval=30)
            logger.info("SSE WebSocket thread has been terminated.")
        except Exception as e:
            logger.error(f"SSE WebSocket error occurred: {e}")
            time.sleep(0.3)
            logger.info("Attempting to reconnect SSE WebSocket...")

# WebSocket 백그라운드 실행 함수 (SSE 전용)
async def run_websocket_background_to_queue(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
    data_queue = asyncio.Queue()
    # 메인 이벤트 루프 참조 전달
    ws_thread = threading.Thread(target=sse_websocket_thread, args=(stock_symbols, data_queue, event_loop))
    ws_thread.start()
    return data_queue


# SSE 전송을 위한 데이터 처리 함수
def process_data_for_sse(data, stock_symbol):
    stock_info = get_company_details(stock_symbol)
    if not stock_info or "id" not in stock_info or "name" not in stock_info:
        print(f"No valid company information for symbol: {stock_symbol}")
        return None

    try:
        d1 = data.split("|")
        if len(d1) >= 4:
            recvData = d1[3]
            result = recvData.split("^")
            if len(result) > 12:
                stock_data = {
                    "id": stock_info.get("id"),
                    "name": stock_info.get("name"),
                    "symbol": stock_symbol,
                    "close": result[2],
                    "rate_price": result[4],
                    "rate": result[5],
                    "volume": result[12],
                }
                return stock_data
            else:
                print(f"Unexpected result format for data: {result}")
    except (IndexError, ValueError, TypeError) as e:
        print(f"Error processing stock data for SSE: {e}")
    return None





