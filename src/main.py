from fastapi import FastAPI, APIRouter
from starlette.middleware.cors import CORSMiddleware
from src.user import routes as user_routes
from src.stock import routes as stock_routes
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from .stock.websocket import run_websocket_background_multiple
from .logger import logger
from .stock.crud import get_symbol_list


# WebSocket 스케줄링 함수
async def schedule_websockets():
    symbol_list = get_symbol_list(1)
    try:
        # 다중 심볼을 한 번의 WebSocket으로 처리하도록 symbol_list 전체를 전달
        await run_websocket_background_multiple(symbol_list)
        logger.debug("WebSocket task completed for multiple stocks.")
    except Exception as e:
        logger.error(f"Error in WebSocket scheduling task: {e}")


# lifespan 핸들러 설정
@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler()

    # 매일 오전 9시에 WebSocket 스케줄링 또는 테스트용 매 분 스케줄링
    # scheduler.add_job(schedule_websockets, CronTrigger(hour=9, minute=0))
    scheduler.add_job(schedule_websockets, CronTrigger(minute="*"))  # 테스트용 매 분 스케줄링

    scheduler.start()

    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler and WebSocket connections are shut down.")
# app = FastAPI(lifespan=lifespan)
app = FastAPI()
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