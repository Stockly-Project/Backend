from fastapi import APIRouter, BackgroundTasks
from .producer import init_kafka_producer
from .websocket import run_websocket_background, start_mock_websocket

router = APIRouter(
    prefix="/api/v1/stockDetails",
    tags=["stockDetails"],
)

@router.get("/start-websocket/{stock_symbol}")
async def start_websocket_connection(stock_symbol: str, background_tasks: BackgroundTasks):
    # 백그라운드에서 WebSocket을 실행 (멀티스레딩 사용)
    background_tasks.add_task(run_websocket_background, stock_symbol)
    return {"message": f"Started WebSocket for {stock_symbol}"}
