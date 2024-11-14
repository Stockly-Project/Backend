import json
from kafka import KafkaProducer
from src.logger import logger

# Kafka Producer 초기화
def init_kafka_producer():
    try:
        producer = KafkaProducer(
            # bootstrap_servers=['kafka:9092'],
            bootstrap_servers=['kafka-broker.stockly.svc.cluster.local:9092'],
            api_version=(2, 8, 0),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # JSON 직렬화
            acks=1  # 리더 파티션에만 확인 응답을 받고 즉시 전송 완료
        )
        logger.info("Kafka producer initialized successfully.")
        return producer
    except Exception as e:
        logger.error(f"Error initializing Kafka producer: {e}")
        return None

# def send_to_kafka(producer, topic, data):
#     if producer is None:
#         logger.error("Kafka producer is not initialized.")
#         return
#     try:
#         # 메시지 전송 및 결과 확인
#         future = producer.send(topic, value=data)
#         result = future.get(timeout=10)  # 전송 확인
#         logger.info(f"Sent to Kafka topic {topic}: {data}")
#         logger.debug(f"Kafka send result: {result}")  # 전송 결과를 로깅
#         producer.flush()  # 모든 버퍼가 전송될 때까지 대기
#     except Exception as e:
#         logger.error(f"Error sending to Kafka: {e}")

# Kafka로 데이터 전송
async def send_to_kafka(producer, topic, data):
    if producer is None:
        logger.error("Kafka producer is not initialized.")
        return
    try:
        # 메시지를 비동기 전송
        producer.send(topic, value=data)
        logger.info(f"Sent to Kafka topic {topic}: {data}")
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")

