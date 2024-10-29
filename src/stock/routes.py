from fastapi import APIRouter, BackgroundTasks, Query
from .websocket import run_websocket_background, start_mock_websocket

# FastAPI 설정
router = APIRouter(
    prefix="/api/v1/stockDetails",
    tags=["stockDetails"],
)

@router.get("/start-websocket/{stock_symbol}")
async def start_websocket_connection(
    stock_symbol: str,
    background_tasks: BackgroundTasks,
):
    # background_tasks.add_task(run_websocket_background, stock_symbol)
    background_tasks.add_task(start_mock_websocket, stock_symbol)
    return {"message": f"Started WebSocket for {stock_symbol}"}