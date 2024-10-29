import json
from kafka import KafkaProducer
from src.logger import logger


# Kafka Producer 초기화
def init_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        api_version=(2, 8, 0),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON 직렬화
    )

def send_to_kafka(producer, topic, data):
    try:
        producer.send(topic, value=data)
        producer.flush()
        logger.info(f"Sent to Kafka topic {topic}: {data}")
    except Exception as e:
        logger.error(f"Error sending to Kafka: {e}")