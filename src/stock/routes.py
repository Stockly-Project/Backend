from fastapi import APIRouter, BackgroundTasks
from .producer import init_kafka_producer
from .websocket import start_websocket, start_mock_websocket

router = APIRouter(
    prefix="/api/v1/stockDetails",
    tags=["stockDetails"],
)

@router.get("/start-websocket/{stock_symbol}")
async def start_websocket_connection(stock_symbol: str, background_tasks: BackgroundTasks):
    producer = init_kafka_producer()
    # WebSocket 작업을 백그라운드에서 실행
    background_tasks.add_task(start_websocket, stock_symbol, producer)
    # background_tasks.add_task(start_mock_websocket, stock_symbol, producer)
    return {"message": f"Started WebSocket for {stock_symbol}"}

