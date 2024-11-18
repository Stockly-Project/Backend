from fastapi import FastAPI, APIRouter
from starlette.middleware.cors import CORSMiddleware
from src.user import routes as user_routes
from src.stock import routes as stock_routes
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from .stock.websocket import run_websocket_background_multiple, run_mock_websocket_background
from .stock.price_websocket import run_asking_websocket_background_multiple
from .logger import logger
from .common.admin_kafka_client import create_kafka_topic
from .stock.crud import get_symbols_for_page

# Kafka 토픽 초기화 함수
async def initialize_kafka():
    # Kafka 토픽을 초기화하는 함수 호출 (토픽이 없다면 생성)
    create_kafka_topic("real_time_stock_prices", num_partitions=1)
    create_kafka_topic("real_time_asking_prices", num_partitions=1)
    logger.info("Kafka topic initialized.")

async def schedule_mock_websockets():
    symbol_list = [{"symbol": symbol} for symbol in get_symbols_for_page(1)]

    try:
        # 목업 WebSocket 실행하여 큐에 데이터 전송
        await run_mock_websocket_background(symbol_list)
        # await run_mock_asking_websocket_background_multiple(symbol_list)
        logger.debug("Mock WebSocket task completed for multiple stocks.")

    except Exception as e:
        logger.error(f"Error in mock WebSocket scheduling task: {e}")

# WebSocket 스케줄링 함수
async def schedule_websockets():
    symbol_list = [{"symbol": symbol} for symbol in get_symbols_for_page(1)]
    logger.error(symbol_list)
    try:
        # 다중 심볼을 한 번의 WebSocket으로 처리하도록 symbol_list 전체를 전달
        await run_websocket_background_multiple(symbol_list)  # Kafka 전송 활성화
        await run_asking_websocket_background_multiple(symbol_list)
        # await run_asking_websocket_background_multiple(symbol_list)
        logger.debug("WebSocket task completed for multiple stocks.")
    except Exception as e:
        logger.error(f"Error in WebSocket scheduling task: {e}")

# lifespan 핸들러 설정
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Kafka 토픽 초기화가 완료될 때까지 대기
    await initialize_kafka()

    await schedule_websockets()
    logger.info("WebSocket scheduling task executed at app startup.")

    # 필요 시 스케줄러를 추가로 사용할 경우
    scheduler = AsyncIOScheduler()
    # scheduler.add_job(schedule_mock_websockets(), CronTrigger(hour=10, minute=0))  # 매일 오전 10시 실행
    scheduler.add_job(schedule_websockets, CronTrigger(minute="*/10"))  # 테스트용 매 분 스케줄링
    scheduler.start()

    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler and WebSocket connections are shut down.")
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


