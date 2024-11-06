import mysql.connector
from src.configs import HOST, USER, PASSWORD, DATABASE, REDIS_URL
import redis.asyncio as aioredis
import os
from mysql.connector import errors
from src.logger import logger
import time

async def get_redis():
    redis_url = os.getenv("REDIS_URL")
    redis = await aioredis.from_url(redis_url)
    return redis

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