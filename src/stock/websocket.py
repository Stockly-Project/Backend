import json
from .kis_configs import get_approval
import os
import websocket
import time
import threading
from .producer import send_to_kafka

# WebSocket 이벤트 핸들러
def on_message(ws, data, producer):
    print(f"Received WebSocket message: {data}")
    try:
        if data[0] in ['0', '1']:  # 시세 데이터일 경우
            d1 = data.split("|")
            if len(d1) >= 4:
                recvData = d1[3]
                result = recvData.split("^")
                stock_data = {
                    "date": result[1],
                    "open": result[7],
                    "close": result[2],
                    "day_high": result[8],
                    "day_low": result[9],
                    "price_change": result[4],
                    "price_change_late": result[5],
                    "volume": result[12],
                    "transaction_volume": result[13],
                    "volume_price": result[14],
                }

                # Kafka로 데이터 전송
                send_to_kafka(producer, 'real_time_stock_prices', stock_data)
            else:
                print(f"Received unexpected data format: {data}")
        else:
            print(f"Received non-stock data: {data}")

    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print('WebSocket error=', error)

def on_close(ws, status_code, close_msg):
    print(f'WebSocket closed with status code={status_code}, message={close_msg}')

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")

def on_open(ws, stock_symbol, producer):
    print(APP_KEY, APP_SECRET, stock_symbol)
    b = {
        "header": {
            "approval_key": get_approval(APP_KEY, APP_SECRET),
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8"
        },
        "body": {
            "input": {
                "tr_id": "H0STCNT0",  # API명
                "tr_key": stock_symbol  # 종목번호
            }
        }
    }
    print('Sending initial WebSocket request')
    ws.send(json.dumps(b), websocket.ABNF.OPCODE_TEXT)

# WebSocket 시작 함수
def start_websocket(stock_symbol, producer):
    print("starting websocket")
    ws = websocket.WebSocketApp(
        "ws://ops.koreainvestment.com:31000",
        on_open=lambda ws: on_open(ws, stock_symbol, producer),
        on_message=lambda ws, data: on_message(ws, data, producer),
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()


# 목데이터 생성 함수
def generate_mock_stock_data(stock_symbol):
    stock_data = {
        "date": "2024-10-22",
        "open": "30000",
        "close": "30500",
        "day_high": "31000",
        "day_low": "29900",
        "price_change": "+500",
        "price_change_late": "+1.67%",
        "volume": "500000",
        "transaction_volume": "15000000",
        "volume_price": "300000000"
    }
    return stock_data


# WebSocket 대신 목데이터를 전송하는 함수
def start_mock_websocket(stock_symbol, producer):
    print("starting mock websocket")

    def mock_send_data():
        while True:
            stock_data = generate_mock_stock_data(stock_symbol)
            # Kafka로 목데이터 전송
            send_to_kafka(producer, 'real_time_stock_prices', stock_data)
            print(f"Sent mock data to Kafka: {stock_data}")
            time.sleep(5)  # 5초마다 목데이터 전송

    # 별도의 스레드에서 목데이터 전송
    threading.Thread(target=mock_send_data).start()