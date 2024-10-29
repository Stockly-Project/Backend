import json
import os
import time
import random
import threading
import asyncio
import websocket
from concurrent.futures import ThreadPoolExecutor
from .producer import send_to_kafka, init_kafka_producer
from .kis_configs import get_approval
from src.logger import logger
from datetime import datetime

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")


# WebSocket 이벤트 핸들러
def on_message(ws, data, producer, stock_symbol):
    print(f"Received WebSocket message: {data}")
    try:
        if data[0] in ['0', '1']:  # 시세 데이터일 경우
            d1 = data.split("|")
            if len(d1) >= 4:
                recvData = d1[3]
                result = recvData.split("^")
                stock_data = {
                    "symbol": stock_symbol,  # 심볼 추가
                    "date": result[1],
                    "open": result[7],
                    "close": result[2],
                    "high": result[8],
                    "low": result[9],
                    "rate_price": result[4],
                    "rate": result[5],
                    "volume": result[12],
                    # "transaction_volume": result[13], # 누적 총 거래량
                    # "volume_price": result[14], # 누적 총 거래 금액
                }

                send_to_kafka(producer, "real_time_stock_prices", stock_data)

            else:
                logger.debug(f"Received unexpected data format: {data}")
        else:
            logger.debug(f"Received non-stock data: {data}")

    except Exception as e:
        logger.error(f"Error processing message: {e}")


def on_error(ws, error):
    logger.error(f'WebSocket error: {error}')


def on_close(ws, status_code, close_msg):
    logger.info(f'WebSocket closed with status code={status_code}, message={close_msg}')


def on_open(ws, stock_symbol, producer):
    approval_key = get_approval(APP_KEY, APP_SECRET)
    user_agent = f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.{random.randrange(99)} (KHTML, like Gecko) Chrome/87.0.4280.67 Safari/537.36"
    request_data = {
        "header": {
            "User-Agent": user_agent,
            "appkey": APP_KEY,
            "appsecret": APP_SECRET,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8"
        },
        "body": {
            "input": {
                "tr_id": "H0STCNT0",
                "tr_key": stock_symbol
            }
        }
    }
    ws.send(json.dumps(request_data), websocket.ABNF.OPCODE_TEXT)
    logger.debug('Sent initial WebSocket request')


# WebSocket 시작 함수
def start_websocket(stock_symbol, producer):
    logger.info(f"Starting WebSocket for stock symbol: {stock_symbol}")
    while True:
        ws = websocket.WebSocketApp(
            "ws://ops.koreainvestment.com:31000",
            on_open=lambda ws: on_open(ws, stock_symbol, producer),
            on_message=lambda ws, data: on_message(ws, data, producer, stock_symbol),
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()
        time.sleep(5)  # 재연결 대기


# 멀티스레딩을 사용하는 WebSocket 백그라운드 작업
async def run_websocket_background(stock_symbol: str):
    loop = asyncio.get_event_loop()
    producer = init_kafka_producer()  # Kafka Producer 초기화
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, start_websocket, stock_symbol, producer)
    logger.debug(f"WebSocket background task started for stock symbol: {stock_symbol}")


# 목데이터 생성 함수
def generate_mock_stock_data(stock_symbol):
    stock_data = {
        "symbol": "005930",
        "date": "135950",
        "open": "30000",
        "close": "30500",
        "high": "31000",
        "low": "29900",
        "rate_price": "500",
        "rate": "1.67",
        "volume": "500000"
    }
    return stock_data


# WebSocket 대신 목데이터를 전송하는 함수
def start_mock_websocket(stock_symbol):
    logger.debug("Starting mock websocket for testing")
    producer = init_kafka_producer()
    def mock_send_data():
        while True:
            stock_data = generate_mock_stock_data(stock_symbol)
            send_to_kafka(producer, "real_time_stock_prices", stock_data)
            logger.debug(f"Sent mock data to Kafka: {stock_data}")
            time.sleep(5)

    threading.Thread(target=mock_send_data).start()