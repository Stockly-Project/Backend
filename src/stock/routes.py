from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from .crud import get_company_by_symbol
from .schemas import CompanyResponse
from .crud import get_symbols_for_page
from .websocket import run_websocket_background_single
from .multi_websocket import run_websocket_background_multiple, sse_event_generator
from fastapi.responses import StreamingResponse

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


@router.get("/stream/multiple")
async def sse_stream_multiple(page: int = Query(1)):
    stocks = get_symbols_for_page(page)  # `stocks`는 `List[Dict[str, str]]` 형태
    data_queue = await run_websocket_background_multiple(stocks)  # `stocks`를 인자로 전달
    return StreamingResponse(sse_event_generator(data_queue), media_type="text/event-stream")