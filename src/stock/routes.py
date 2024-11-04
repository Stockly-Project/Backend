from fastapi import APIRouter, HTTPException, Query
from .crud import get_company_by_symbol
from .schemas import CompanyResponse
from .websocket import run_websocket_background_multiple
from fastapi.responses import StreamingResponse
from .crud import get_symbols_for_page

# FastAPI 설정
router = APIRouter(
    prefix="/api/v1/stockDetails",
    tags=["stockDetails"],
)

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


# @router.get("/stream/multiple")
# async def sse_stream_multiple(page: int = Query(1)):
#     # 심볼 목록을 가져와서 WebSocket 연결 시작
#     symbols = [{"symbol": symbol} for symbol in get_symbols_for_page(page)]  # 페이지에 따른 심볼 목록 가져오기
#     data_queue = await run_websocket_background_multiple(symbols)
#     return StreamingResponse(sse_event_generator(data_queue), media_type="text/event-stream")