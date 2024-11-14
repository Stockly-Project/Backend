from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from src.logger import logger

def create_kafka_topic(topic_name, num_partitions=8, replication_factor=1):
    # Kafka Admin Client 초기화
    admin_client = KafkaAdminClient(
        # bootstrap_servers=['kafka:9092'],
        bootstrap_servers=['kafka-broker.stockly.svc.cluster.local:9092'],
        client_id='kafka-python-admin'
    )

    # NewTopic 객체 생성
    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    try:
        # 토픽 생성 시도
        admin_client.create_topics([topic])
        logger.info(f"Topic '{topic_name}' created with {num_partitions} partitions.")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic_name}' already exists. Skipping creation.")
    except Exception as e:
        logger.error(f"Error creating topic '{topic_name}': {e}")
    finally:
        admin_client.close()