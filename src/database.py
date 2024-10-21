import mysql.connector
from src.configs import HOST, USER, PASSWORD, DATABASE, REDIS_URL
import redis.asyncio as aioredis

# Redis 클라이언트 생성
async def get_redis():
    redis_url = REDIS_URL
    redis = await aioredis.from_url(redis_url)
    return redis

#MySQL 데이터베이스에 연결
def get_db_connection():
    connection = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )
    return connection
