from fastapi import FastAPI, APIRouter
from starlette.middleware.cors import CORSMiddleware
from src.user import routes as user_routes
from src.stock import routes as stock_routes
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from .stock.websocket import run_websocket_background_multiple, run_websocket_background_multiple_mock
from .stock.price_websocket import run_asking_websocket_background_multiple, run_asking_websocket_background_multiple_mock
from .logger import logger
from .common.admin_kafka_client import create_kafka_topic
from .stock.crud import get_symbols_for_page
import asyncio
from .database import get_db_connection
import mysql.connector

# Kafka 토픽 초기화 함수
async def initialize_kafka():
    # Kafka 토픽을 초기화하는 함수 호출 (토픽이 없다면 생성)
    create_kafka_topic("real_time_stock_prices", num_partitions=15)
    create_kafka_topic("real_time_asking_prices", num_partitions=5)
    create_kafka_topic("one_minutes_stock_prices", num_partitions=5)
    logger.info("Kafka topic initialized.")
    

async def schedule_websockets():
    symbol_list = [{"symbol": symbol} for symbol in get_symbols_for_page(1)]
    logger.error(symbol_list)
    try:
        logger.debug("Starting WebSocket tasks...")
        await asyncio.gather(
            # run_websocket_background_multiple_mock(symbol_list),
            # run_asking_websocket_background_multiple_mock(symbol_list),
            run_websocket_background_multiple(symbol_list),
            # run_asking_websocket_background_multiple(symbol_list),
        )
        logger.debug("Both WebSocket tasks completed successfully.")
    except Exception as e:
        logger.error(f"Error in WebSocket scheduling task: {e}")


async def run_websocket_tasks(database: mysql.connector.MySQLConnection = None):
    """WebSocket 작업 실행."""
    symbol_list = [{"symbol": symbol} for symbol in get_symbols_for_page(1,20, database)]
    logger.debug(f"Starting WebSocket tasks with symbols: {symbol_list}")
    try:
        await asyncio.gather(
            # run_websocket_background_multiple(symbol_list),
            run_websocket_background_multiple_mock(symbol_list),
        )
    except Exception as e:
        logger.error(f"Error running WebSocket tasks: {e}")


# lifespan 핸들러 설정
@asynccontextmanager
async def lifespan(app: FastAPI):
    """앱 시작/종료 시 설정."""
    connection = get_db_connection()
    # Kafka 토픽 초기화
    await initialize_kafka()

    # WebSocket 작업 실행
    asyncio.create_task(run_websocket_tasks(connection))

    # 스케줄러 초기화 및 시작
    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_websocket_tasks, CronTrigger(minute="*/10"))  # 10분마다 실행
    scheduler.start()
    logger.info("Scheduler started and WebSocket tasks scheduled.")

    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shut down.")

        
app = FastAPI(lifespan=lifespan)
# app = FastAPI()
router = APIRouter(prefix="/api/v1")
app.include_router(user_routes.router)
app.include_router(stock_routes.router)

# CORS 미들웨어 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def hello():
    return {"message": "메인페이지입니다"}
