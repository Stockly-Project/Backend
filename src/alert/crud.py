import json
from src.database import get_db_connection
from src.logger import logger
from src.common.producer import init_kafka_producer


async def create_notification_prices(notification_data):
    """
    알림 조건을 데이터베이스에 저장하고, Kafka에 이벤트를 발행합니다.
    """
    connection = None
    producer = None
    try:
        # 데이터베이스 연결
        connection = get_db_connection()
        cursor = connection.cursor()

        # `company` 테이블에서 `symbol`로 `company_id`와 `name` 조회
        company_query = "SELECT id, name FROM company WHERE symbol = %s AND is_deleted = FALSE"
        cursor.execute(company_query, (notification_data["symbol"],))
        company = cursor.fetchone()

        if not company:
            raise ValueError(f"Company with symbol {notification_data['symbol']} not found")

        company_id, company_name = company  # 조회된 회사 정보 분리

        # 데이터 삽입 쿼리
        insert_query = """
        INSERT INTO notification (user_id, company_id, price, is_active)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            notification_data["user_id"],
            company_id,
            notification_data["price"],
            False
        ))

        # 생성된 notification_id 가져오기
        notification_id = cursor.lastrowid

        # 커밋
        connection.commit()

        # Kafka에 이벤트 발행
        producer = await init_kafka_producer()
        kafka_message = {
            "notification_id": notification_id,
            "user_id": notification_data["user_id"],
            "company_id": company_id,
            "company_name": company_name,  # 회사 이름 추가
            "symbol": notification_data["symbol"],  # 심볼 추가
            "target_price": notification_data["price"],
            "is_active": False
        }
        await producer.send("stock_prices_alert", json.dumps(kafka_message).encode("utf-8"))

        logger.info(f"Notification created and published to Kafka: {kafka_message}")

        return {
            "code": 200,
            "message": "가격 알림 설정 성공",
            "notification_id": notification_id,
            "user_id": notification_data["user_id"],
            "company_id": company_id,
            "company_name": company_name,
            "price": notification_data["price"]
        }

    except Exception as e:
        logger.error(f"Failed to create notification: {e}")
        if connection:
            connection.rollback()
        raise e
    finally:
        # Kafka 프로듀서 명시적으로 닫기
        if producer is not None:
            await producer.stop()
        if connection:
            cursor.close()
            connection.close()