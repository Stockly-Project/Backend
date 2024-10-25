import json
from kafka import KafkaProducer
from src.logger import logger

# Kafka Producer 초기화
def init_kafka_producer():
    return KafkaProducer(
        # bootstrap_servers=['kafka:9092'],  # 도커 컴포즈로 작업 시 Kafka 브로커 주소
        bootstrap_servers=['kafka-broker.stockly.svc.cluster.local:9092'],
        api_version=(2, 8, 0),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON 직렬화
    )

# Kafka에 메시지 전송 함수
def send_to_kafka(producer, stock_symbol, data):
    try:
        data["job_id"] = "stock_chart_detail"
        topic = f"real_time_stock_prices_{stock_symbol}"  # 심볼 기반으로 토픽 설정
        producer.send(topic, data)
        producer.flush()
        logger.info(f"Sent to Kafka topic {topic}: {data}")
    except Exception as e:
        logger.info(f"Error sending to Kafka: {e}")