from fastapi import FastAPI, APIRouter
from starlette.middleware.cors import CORSMiddleware
from src.user import routes as user_routes
from src.stock import routes as stock_routes
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from .stock.websocket import run_websocket_background, start_mock_websocket
import asyncio
from .logger import logger

# 미리 지정된 주식 종목 리스트
stocks_to_track = ['005930']  # 삼성전자


# WebSocket 스케줄링 함수
def schedule_websockets():
    loop = asyncio.new_event_loop()  # 새로운 이벤트 루프 생성
    asyncio.set_event_loop(loop)
    tasks = []

    # 기본 필터링 주기 예: 1m
    interval = "1m"

    for stock_symbol in stocks_to_track:
        # task = loop.create_task(run_websocket_background(stock_symbol))
        task = loop.create_task(start_mock_websocket(stock_symbol))
        task.add_done_callback(
            lambda t: logger.debug(f"Task completed for stock: {stock_symbol}"))
        tasks.append(task)

    loop.run_until_complete(asyncio.gather(*tasks))  # 모든 작업 완료까지 대기
    logger.debug("Started WebSocket for all predefined stocks with interval filtering")


# lifespan 핸들러 설정
@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler()

    # 매일 오전 9시 또는 매 분마다 작업 실행
    # scheduler.add_job(schedule_websockets, CronTrigger(hour=9, minute=0))
    scheduler.add_job(schedule_websockets, CronTrigger(minute="*"))  # 테스트용 매 분 스케줄링

    scheduler.start()
    logger.debug("Scheduler started to track stocks at 9 AM daily")

    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)
# app = FastAPI()
router = APIRouter(prefix="/api/v1")
app.include_router(user_routes.router)
app.include_router(stock_routes.router)

# CORS 미들웨어 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def hello():
    return {"message": "메인페이지입니다"}