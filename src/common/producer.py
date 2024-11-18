import json
import asyncio
from aiokafka import AIOKafkaProducer
from src.logger import logger

TOPIC_STOCK_DATA = "real_time_stock_prices"

# Kafka Producer 초기화
async def init_kafka_producer():
    try:
        producer = AIOKafkaProducer(
            # bootstrap_servers=['kafka:9092'],
            bootstrap_servers=['kafka-broker.stockly.svc.cluster.local:9092'],
            api_version=(2, 8, 0),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON 직렬화
        )
        # Producer를 시작 (connection 및 준비 단계)
        await producer.start()
        logger.info("Kafka producer initialized successfully.")
        return producer
    except Exception as e:
        logger.error(f"Error initializing Kafka producer: {e}")
        return None

# Kafka로 데이터 전송
async def send_to_kafka(producer, topic, data):
    if producer is None:
        logger.error("Kafka producer is not initialized.")
        return
    try:
        # 비동기적으로 메시지 전송
        await producer.send_and_wait(topic, value=data)
        logger.info(f"Sent to Kafka topic {topic}: {data}")
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")

# Producer를 닫는 함수
async def close_kafka_producer(producer):
    if producer:
        await producer.stop()
        logger.info("Kafka producer closed successfully.")
