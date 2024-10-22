import mysql.connector
from src.configs import HOST, USER, PASSWORD, DATABASE, REDIS_URL
import redis.asyncio as aioredis
import os

# Redis 클라이언트 생성
# async def get_redis():
#     redis_url = REDIS_URL
#     redis = await aioredis.from_url(redis_url)
#     return redis

#MySQL 데이터베이스에 연결
# def get_db_connection():
#     connection = mysql.connector.connect(
#         host=HOST,
#         user=USER,
#         password=PASSWORD,
#         database=DATABASE
#     )
#     return connection


async def get_redis():
    redis_host = os.getenv("REDIS_HOST")
    redis_port = os.getenv("REDIS_PORT")
    redis_url = f"redis://{redis_host}:{redis_port}"
    redis = await aioredis.from_url(redis_url)
    return redis

def get_db_connection():
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")

    # 디버깅용 출력
    print(f"Connecting to MySQL with USER: {user}, PASSWORD: {password}, DATABASE: {database}")

    connection = mysql.connector.connect(
        host="mysql",
        user=user,
        password=password,
        database=database
    )
    return connection