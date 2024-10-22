from fastapi import APIRouter
from fastapi.responses import StreamingResponse
import asyncio
from kafka import KafkaConsumer
from .producer import start_websocket, init_kafka_producer  # producer.py에서 WebSocket 관련 함수 import
import json
from concurrent.futures import ThreadPoolExecutor


router = APIRouter(
    prefix="/api/v1/stockDetails",
    tags=["stockDetails"],
)

# Kafka Consumer 설정
def kafka_consumer(stock_symbol: str):
    return KafkaConsumer(
        'real_time_stock_prices',
        bootstrap_servers=['kafka:9092'],  # 도커 컴포즈로 작업 시 Kafka 브로커 주소
        auto_offset_reset='earliest',  # 이 부분에서 'earliest'는 처음부터 메시지를 읽음
        group_id=f'{stock_symbol}_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

# WebSocket 백그라운드 작업 실행
async def run_websocket_background(stock_symbol: str):
    loop = asyncio.get_event_loop()
    producer = init_kafka_producer()  # Kafka Producer 초기화
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, start_websocket, stock_symbol, producer)
    print(f"WebSocket background task started for stock symbol: {stock_symbol}")

# 종목 코드에 따른 SSE 실시간 스트리밍 함수
@router.get("/{stock_symbol}/stream", response_class=StreamingResponse)
async def stream_stock_data(stock_symbol: str):
    print(f"Received SSE request for stock symbol: {stock_symbol}")

    # WebSocket 데이터를 백그라운드에서 실행
    asyncio.create_task(run_websocket_background(stock_symbol))

    async def event_generator():
        print("Initializing Kafka consumer...")
        consumer = kafka_consumer(stock_symbol)  # Kafka Consumer 초기화
        print(f"Kafka consumer created for stock symbol: {stock_symbol}")

        try:
            # Kafka 메시지를 지속적으로 읽어오기
            for message in consumer:
                stock_data = message.value
                if stock_data:
                    # SSE로 클라이언트에 메시지 전송
                    yield f"data: {json.dumps(stock_data)}\n\n"
                    print(f"Sending data to SSE for stock symbol {stock_symbol}: {stock_data}")
                    await asyncio.sleep(0.1)  # 메시지 처리 속도 조절
                else:
                    print("No data received from Kafka.")
        except Exception as e:
            print(f"Error in Kafka consumer or SSE generator: {e}")
        finally:
            # 종료 처리
            consumer.close()
            print(f"Kafka consumer closed for stock symbol: {stock_symbol}")

    return StreamingResponse(event_generator(), media_type="text/event-stream")