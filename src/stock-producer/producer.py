import json
import websocket
from kafka import KafkaProducer
from kis_configs import get_approval
from src.configs import APP_SECRET, APP_KEY

# Kafka Producer 설정
producer = KafkaProducer(
    bootstrap_servers=['localhost:9094'],  # Kafka 브로커 주소
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON 직렬화
)

# 실제 사용 중인 appkey와 secretkey
g_appkey = APP_KEY
g_appsecret = APP_SECRET

# approval_key를 가져옴
g_approval_key = get_approval(g_appkey, g_appsecret)

h = {
    "appkey": g_appkey,
    "appsecret": g_appsecret
}
b = {
    "header": {
        "approval_key": g_approval_key,
        "custtype": "P",
        "tr_type": "1",
        "content-type": "utf-8"
    },
    "body": {
        "input": {
            "tr_id": "H0STCNT0",  # API명
            "tr_key": "373220"    # 종목번호
        }
    }
}

def on_message(ws, data):
    if data[0] in ['0', '1']:  # 시세 데이터일 경우
        d1 = data.split("|")
        if len(d1) >= 4:
            recvData = d1[3]
            result = recvData.split("^")
            stock_data = {
                "date": result[1],
                "open": result[7], # 시가
                "close": result[2], # 종가(현재가)
                "day_high": result[8], # 1일 최고가
                "day_low": result[9], # 1일 최저가
                "price_change": result[4], # 가격 상승/하락폭
                "price_change_late": result[5], # 등락률
                "volume": result[12], # 실시간 체결 거래량
                "transaction_volume": result[13],# 당일 누적 거래량
                "volume_price": result[14], # 당일 누적 거래 대금
            }
            # Kafka로 데이터 전송
            producer.send('real_time_stock_prices', stock_data)
            producer.flush()

            # 콘솔 출력 (디버깅용)
            print(f"Sent to Kafka: {stock_data}")
        else:
            print('Data Size Error=', len(d1))
    else:
        recv_dic = json.loads(data)
        tr_id = recv_dic['header']['tr_id']

        if tr_id == 'PINGPONG':
            ws.send(data, websocket.ABNF.OPCODE_PING)
        else:
            print('tr_id=', tr_id, '\nmsg=', data)


# WebSocket 연결 에러 처리
def on_error(ws, error):
    print('error=', error)


# WebSocket 연결 종료 처리
def on_close(ws, status_code, close_msg):
    print('on_close close_status_code=', status_code, " close_msg=", close_msg)


# WebSocket 연결 시 초기화 작업
def on_open(ws):
    print('on_open send data=', json.dumps(b))
    ws.send(json.dumps(b), websocket.ABNF.OPCODE_TEXT)


# WebSocket 연결 설정
ws = websocket.WebSocketApp("ws://ops.koreainvestment.com:31000",
                            on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)

# WebSocket 실행
ws.run_forever()
