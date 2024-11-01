# import json
# import os
# import time
# import random
# import threading
# import asyncio
# import websocket
# from concurrent.futures import ThreadPoolExecutor
# from .producer import send_to_kafka, init_kafka_producer
# from .kis_configs import get_approval
# from src.logger import logger
#
# APP_KEY = os.getenv("APP_KEY")
# APP_SECRET = os.getenv("APP_SECRET")
#
# # 주식 데이터 처리 함수
# def process_stock_data(data, stock_symbol):
#     d1 = data.split("|")
#     if len(d1) >= 4:
#         recvData = d1[3]
#         result = recvData.split("^")
#         stock_data = {
#             "symbol": stock_symbol,
#             "date": result[1],
#             "open": result[7],
#             "close": result[2],
#             "high": result[8],
#             "low": result[9],
#             "rate_price": result[4],
#             "rate": result[5],
#             "volume": result[12],
#         }
#         return stock_data
#     return None
#
# # 호가 데이터 처리 함수
# def process_stock_price_data(data, stock_symbol):
#     recvvalue = data.split('^')
#     stock_price_data = {
#         "symbol": stock_symbol,
#         "sell_prices": [recvvalue[i] for i in range(3, 13)],
#         "sell_volumes": [recvvalue[i] for i in range(23, 33)],
#         "buy_prices": [recvvalue[i] for i in range(13, 23)],
#         "buy_volumes": [recvvalue[i] for i in range(33, 43)],
#         "total_sell_volume": recvvalue[43],
#         "total_buy_volume": recvvalue[44]
#     }
#     return stock_price_data
#
#
# # on_message 함수에서 메시지 종류에 따라 처리 함수 호출
# def on_message(ws, data, producer, stock_symbol):
#     print(f"Received WebSocket message: {data}")
#     try:
#         if data.startswith("H0STASP0"):  # 호가 데이터일 경우
#             stock_price_data = process_stock_price_data(data, stock_symbol)
#             if stock_price_data:
#                 send_to_kafka(producer, "real_time_asking_prices", stock_price_data)
#
#         elif data[0] in ['0', '1']:  # 주식 가격 데이터일 경우
#             stock_data = process_stock_data(data, stock_symbol)
#             if stock_data:
#                 send_to_kafka(producer, "real_time_stock_prices", stock_data)
#
#         else:
#             logger.debug(f"Received non-stock data: {data}")
#
#     except Exception as e:
#         logger.error(f"Error processing message: {e}")
#
# def on_error(ws, error):
#     logger.error(f'WebSocket error: {error}')
#
#
# def on_close(ws, status_code, close_msg):
#     logger.info(f'WebSocket closed with status code={status_code}, message={close_msg}')
#
#
# def on_open(ws, stock_symbol, producer):
#     approval_key = get_approval(APP_KEY, APP_SECRET)
#     user_agent = f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.{random.randrange(99)} (KHTML, like Gecko) Chrome/87.0.4280.67 Safari/537.36"
#     request_data = {
#         "header": {
#             "User-Agent": user_agent,
#             "appkey": APP_KEY,
#             "appsecret": APP_SECRET,
#             "custtype": "P",
#             "tr_type": "1",
#             "content-type": "utf-8"
#         },
#         "body": {
#             "input": {
#                 "tr_id": "H0STCNT0",
#                 "tr_key": stock_symbol
#             }
#         }
#     }
#     ws.send(json.dumps(request_data), websocket.ABNF.OPCODE_TEXT)
#     logger.debug('Sent initial WebSocket request')
#

import json
import os
import asyncio
import websocket
from concurrent.futures import ThreadPoolExecutor
from .producer import send_to_kafka, init_kafka_producer
from .kis_configs import get_approval
from src.logger import logger
from fastapi import APIRouter
import time
import random
from typing import List, Dict

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")

router = APIRouter()

# WebSocket 에러 및 종료 핸들러
def on_error(ws, error):
    logger.error(f'WebSocket error: {error}')

def on_close(ws, status_code, close_msg):
    logger.info(f'WebSocket closed with status code={status_code}, message={close_msg}')

# 주식 데이터 처리 함수 - 단일 회사
def process_single_stock_data(data, stock_symbol):
    d1 = data.split("|")
    if len(d1) >= 4:
        recvData = d1[3]
        result = recvData.split("^")
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
    return None

# 호가 데이터 처리 함수 - 단일 회사
def process_single_stock_price_data(data, stock_symbol):
    recvvalue = data.split('^')
    stock_price_data = {
        "symbol": stock_symbol,
        "sell_prices": [recvvalue[i] for i in range(3, 13)],
        "sell_volumes": [recvvalue[i] for i in range(23, 33)],
        "buy_prices": [recvvalue[i] for i in range(13, 23)],
        "buy_volumes": [recvvalue[i] for i in range(33, 43)],
        "total_sell_volume": recvvalue[43],
        "total_buy_volume": recvvalue[44]
    }
    return stock_price_data

# WebSocket 이벤트 핸들러 - 단일 회사
def on_message_single_company(ws, data, producer, stock_symbol):
    try:
        if data.startswith("H0STASP0"):  # 호가 데이터일 경우
            stock_price_data = process_single_stock_price_data(data, stock_symbol)
            if stock_price_data:
                send_to_kafka(producer, "real_time_asking_prices", stock_price_data)
        elif data[0] in ['0', '1']:  # 주식 가격 데이터일 경우
            stock_data = process_single_stock_data(data, stock_symbol)
            if stock_data:
                send_to_kafka(producer, "real_time_stock_prices", stock_data)
    except Exception as e:
        logger.error(f"Error processing single company message: {e}")

# WebSocket 연결 설정 - 단일 회사
def on_open_single_company(ws, stock_symbol, producer):
    approval_key = get_approval(APP_KEY, APP_SECRET)
    request_data_price = {
        "header": {
            "approval_key": approval_key,
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
    request_data_hoga = {
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8"
        },
        "body": {
            "input": {
                "tr_id": "H0STASP0",
                "tr_key": stock_symbol
            }
        }
    }
    ws.send(json.dumps(request_data_price))
    ws.send(json.dumps(request_data_hoga))
    logger.debug(f'Sent WebSocket request for single company: {stock_symbol}')

# WebSocket 시작 함수 - 단일 회사
def start_websocket_single_company(stock_symbol, producer):
    logger.info(f"Starting WebSocket for stock symbol: {stock_symbol}")
    while True:
        ws = websocket.WebSocketApp(
            "ws://ops.koreainvestment.com:31000",
            on_open=lambda ws: on_open_single_company(ws, stock_symbol, producer),
            on_message=lambda ws, data: on_message_single_company(ws, data, producer, stock_symbol),
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()
        time.sleep(5)

# WebSocket 백그라운드 실행 - 단일 회사
async def run_websocket_background_single(stock_symbol):
    producer = init_kafka_producer()
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, start_websocket_single_company, stock_symbol, producer)

# Mock 데이터 생성 - 단일 회사
def generate_mock_single_stock_data(stock_symbol):
    stock_data = {
        "symbol": stock_symbol,
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

# Mock 데이터 전송 - 단일 회사
async def mock_websocket_single_company(stock_symbol, producer):
    while True:
        stock_data = generate_mock_single_stock_data(stock_symbol)
        send_to_kafka(producer, "real_time_stock_prices", stock_data)
        logger.debug(f"Sent mock data to Kafka for single company: {stock_data}")
        await asyncio.sleep(1)


# WebSocket 백그라운드 실행 - 단일 회사 (Mock)
async def run_mock_websocket_background_single(stock_symbol):
    producer = init_kafka_producer()
    await mock_websocket_single_company(stock_symbol, producer)

