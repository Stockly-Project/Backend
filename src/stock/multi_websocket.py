import json
import asyncio
import websocket
import threading
from typing import Dict, List
import os
from .kis_configs import get_approval
from src.logger import logger
import random
import time

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")

# WebSocket 에러 및 종료 핸들러
def on_error(ws, error):
    logger.error(f'WebSocket error: {error}')

def on_close(ws, status_code, close_msg):
    logger.info(f'WebSocket closed with status code={status_code}, message={close_msg}')


# 주식 데이터 처리 함수 - 다중 회사
def process_multiple_stock_data(data, stock_info):
    try:
        # `data`가 문자열로 전달되었는지 확인
        if not isinstance(data, str):
            logger.error(f"Expected `data` as str, but got {type(data)} with value {data}")
            return None

        # `stock_info`가 딕셔너리 형식인지 확인
        if not isinstance(stock_info, dict):
            logger.error(f"Expected `stock_info` as dict, but got {type(stock_info)} with value {stock_info}")
            return None

        # 데이터 분리
        d1 = data.split("|")
        if len(d1) >= 4:
            recvData = d1[3]
            result = recvData.split("^")

            # result 길이를 확인하여 필요한 데이터가 있는지 확인
            if len(result) > 12:
                try:
                    # 문자열 데이터를 숫자 타입으로 변환
                    close = float(result[2])
                    rate_price = float(result[4])
                    rate = float(result[5])
                    volume = int(result[12])

                    # stock_info에서 값 추출 (이때 'id'는 int, 'name'과 'symbol'은 str로 추출)
                    stock_data = {
                        "id": int(stock_info.get("id", 0)),  # 기본값 0 설정
                        "name": stock_info.get("name", ""),
                        "symbol": stock_info.get("symbol", ""),
                        "close": close,
                        "rate_price": rate_price,
                        "rate": rate,
                        "volume": volume,
                        "volume_price": volume * close
                    }
                    return stock_data
                except (ValueError, IndexError) as e:
                    logger.error(f"Error converting data types in result: {result}, error: {e}")
            else:
                logger.error(f"Unexpected result format for data: {result}")
    except (IndexError, ValueError, TypeError) as e:
        logger.error(f"Error processing stock data for {stock_info}: {e}")
    return None


# WebSocket 연결 설정 - 다중 회사
def on_open_multiple_companies(ws, stock_symbols):
    approval_key = get_approval(APP_KEY, APP_SECRET)
    for stock in stock_symbols:  # `stock_symbols` 리스트의 각 항목에서 `symbol` 값을 꺼냄
        request_data = {
            "header": {
                "approval_key": approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8"
            },
            "body": {
                "input": {
                    "tr_id": "H0STCNT0",
                    "tr_key": stock["symbol"]  # 각 `stock` 딕셔너리의 `symbol` 키값을 사용
                }
            }
        }
        ws.send(json.dumps(request_data))
        logger.debug(f"Sent WebSocket request for company: {stock['symbol']}")
        time.sleep(0.1)

# WebSocket 메시지 핸들러 - 다중 회사
def on_message(ws, message, stock_symbols: List[Dict[str, str]], data_queue, loop):
    try:
        if message[0] in ['0', '1']:  # 주식 가격 데이터일 경우
            batch_data = []
            for stock_info in stock_symbols:
                if isinstance(stock_info, dict):  # `stock_info`가 dict인지 확인
                    stock_data = process_multiple_stock_data(message, stock_info)
                    if stock_data:
                        batch_data.append(stock_data)
                else:
                    logger.error(f"Expected `stock_info` as dict, but got {type(stock_info)} with value {stock_info}")
            if batch_data:
                # 이벤트 루프를 통해 안전하게 큐에 데이터를 추가
                loop.call_soon_threadsafe(asyncio.create_task, data_queue.put(batch_data))
    except Exception as e:
        logger.error(f"Error processing multiple company message: {e}")

# WebSocket을 실행할 스레드 함수 - 다중 회사
def websocket_thread(stock_symbols, data_queue, loop):
    ws = websocket.WebSocketApp(
        "ws://ops.koreainvestment.com:31000",
        on_open=lambda ws: on_open_multiple_companies(ws, stock_symbols),
        on_message=lambda ws, message: on_message(ws, message, stock_symbols, data_queue, loop),
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever(ping_interval=60)

# WebSocket을 백그라운드에서 실행하는 함수 - 다중 회사
async def run_websocket_background_multiple(stock_symbols: List[Dict[str, str]]) -> asyncio.Queue:
    data_queue = asyncio.Queue()  # 새로운 큐 생성
    loop = asyncio.get_running_loop()
    ws_thread = threading.Thread(target=websocket_thread, args=(stock_symbols, data_queue, loop))  # `stock_symbols` 사용
    ws_thread.start()
    return data_queue  # 생성한 큐 반환

# SSE 이벤트 생성기
async def sse_event_generator(data_queue: asyncio.Queue):
    while True:
        data = await data_queue.get()  # 비동기적으로 WebSocket 데이터를 가져옴
        yield f"data: {json.dumps(data)}\n\n"
        await asyncio.sleep(0.3)  # 약간의 딜레이를 주어 안정성 보장

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