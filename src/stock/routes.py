from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from .crud import get_company_by_symbol
from .schemas import CompanyResponse
from .crud import get_symbols_for_page
import websocket
from .websocket import run_websocket_background_multiple, sse_event_generator, run_websocket_background_single, run_mock_websocket_background_multiple
from fastapi.responses import StreamingResponse
from src.logger import logger

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
    background_tasks.add_task(run_websocket_background_single, stock_symbol)
    return {"message": f"Started WebSocket for {stock_symbol}"}



@router.get("/info", response_model=CompanyResponse)
async def get_company_info(symbol: str):
    company = get_company_by_symbol(symbol=symbol)
    if not company:
        raise HTTPException(status_code=404, detail="회사를 찾을 수 없습니다.")

    return {
        "code": 200,
        "message": "회사 정보를 조회했습니다.",
        "data": {
            "company_id": company["id"],
            "name": company["name"],
            "symbol": company["symbol"]
        }
    }


# SSE 엔드포인트 - 다중 회사 : 필터링 로직 필요함
@router.get("/stream/multiple")
async def sse_stream_multiple(page: int = Query(1)):
    stock_symbols = get_symbols_for_page(page)
    # data_queue = await run_mock_websocket_background_multiple(stock_symbols)
    data_queue = await run_websocket_background_multiple(stock_symbols)
    return StreamingResponse(sse_event_generator(data_queue), media_type="text/event-stream")