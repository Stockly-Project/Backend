import json
import asyncio
from aiokafka import AIOKafkaProducer
from src.logger import logger
import random
import orjson


async def init_kafka_producer():
    try:
        # bootstrap_servers = ['kafka:9092']
        bootstrap_servers=['kafka-broker.stockly.svc.cluster.local:9092']

        if isinstance(bootstrap_servers, str):
            bootstrap_servers = [bootstrap_servers]

        # Kafka Producer 초기화
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: orjson.dumps(v) if isinstance(v, dict) else v  # JSON 직렬화
            # key_serializer=lambda k: k.encode('utf-8')
        )
        await producer.start()
        logger.info("Kafka producer initialized successfully.")
        return producer
    except Exception as e:
        logger.error(f"Error initializing Kafka producer: {e}")


async def init_kafka_producer_faust():
    try:
        bootstrap_servers = ['kafka:9092']
        if isinstance(bootstrap_servers, str):
            bootstrap_servers = [bootstrap_servers]

        # Kafka Producer 초기화
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: v if isinstance(v, bytes) else orjson.dumps(v.__dict__)  # faust.Record 직렬화
        )
        await producer.start()
        logger.info("Kafka producer initialized successfully.")
        return producer
    except Exception as e:
        logger.error(f"Error initializing Kafka producer: {e}")


# Producer를 닫는 함수
async def close_kafka_producer(producer):
    if producer:
        await producer.stop()
        logger.info("Kafka producer closed successfully.")

async def init_producer_pool(pool_size):
    return [await init_kafka_producer() for _ in range(pool_size)]

async def send_to_kafka_with_pool(producers, topic, data):
    producer = random.choice(producers)  # Producer 풀에서 랜덤 선택
    try:
        # 데이터 직렬화: orjson 사용
        if not isinstance(data, bytes):  # 이미 직렬화된 경우 처리하지 않음
            data = orjson.dumps(data)

        # Kafka로 데이터 전송
        await producer.send_and_wait(topic, value=data)
        logger.info(f"Sent to Kafka topic {topic}: {data}")
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")


async def send_to_kafka(producer, topic, data):
    if producer is None:
        logger.error("Kafka producer is not initialized.")
        return
    try:
        if isinstance(data, bytes):
            serialized_data = data  # 이미 직렬화된 경우 그대로 사용
        else:
            serialized_data = orjson.dumps(data)  # 빠른 직렬화

        await producer.send_and_wait(topic, value=serialized_data)
        logger.info(f"Sent to Kafka topic {topic}: {serialized_data}")
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")