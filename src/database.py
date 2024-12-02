import mysql.connector
from src.configs import HOST, USER, PASSWORD, DATABASE, REDIS_URL
import redis.asyncio as aioredis
import os
from mysql.connector import errors
from src.logger import logger
import time

async def get_redis():
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        logger.error("REDIS_URL 환경 변수가 설정되지 않았습니다.")
        raise ValueError("REDIS_URL 환경 변수를 설정하세요.")
    
    try:
        logger.debug(f"Redis 연결 시도: {redis_url}")
        redis = await aioredis.from_url(redis_url)
        logger.debug("Redis 연결 성공")
        return redis
    except Exception as e:
        logger.error(f"Redis 연결 실패: {str(e)}")
        raise

def get_db_connection():
    user = os.getenv("MYSQL_USER")
    host = "mysql"
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")

    retries = 5  # 최대 재시도 횟수
    while retries > 0:
        try:
            connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                charset="utf8mb4",
                use_unicode=True
            )
            return connection
        except errors.InterfaceError as e:
            retries -= 1
            logger.warning(f"MySQL connection failed, retrying... ({5 - retries}/5)")
            time.sleep(5)
            if retries == 0:
                logger.error("Could not connect to MySQL after multiple attempts.")
                raise e