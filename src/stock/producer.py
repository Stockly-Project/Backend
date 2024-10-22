import json
from kafka import KafkaProducer
from .kis_configs import get_approval
from src.configs import APP_SECRET, APP_KEY
import os

try:
    import websocket
except ImportError:
    print("websocket-client 설치중입니다.")
    os.system('python3 -m pip install websocket-client')


# Kafka Producer 초기화
def init_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],  # Kafka 브로커 주소
        api_version=(2, 0, 0),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON 직렬화
    )

# Kafka에 메시지 전송 함수
def send_to_kafka(producer, topic, data):
    try:
        producer.send(topic, data)
        producer.flush()
        print(f"Sent to Kafka: {data}")
    except Exception as e:
        print(f"Error sending to Kafka: {e}")

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

def on_open(ws, stock_symbol, producer):
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
    print("데이터 내놔 제발")
    ws.run_forever()