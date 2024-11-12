from fastapi import APIRouter, HTTPException, Query
from .crud import get_company_by_symbol
from .schemas import CompanyResponse
from .sse_websocket import run_websocket_background_to_queue
from .crud import get_symbols_for_page
from fastapi.responses import StreamingResponse
import json
import asyncio
from src.logger import logger

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
#     symbols = [{"symbol": symbol} for symbol in get_symbols_for_page(page)]
#
#     async def event_generator():
#         data_queue = await run_websocket_background_to_queue(symbols)
#         symbol_data_dict = {}
#
#         while True:
#             # 1초 동안 데이터를 수집
#             start_time = asyncio.get_event_loop().time()
#             while len(symbol_data_dict) < 20 and (asyncio.get_event_loop().time() - start_time) < 1:
#                 try:
#                     # 큐에서 데이터를 받아와 중복 없이 수집
#                     mock_data = await data_queue.get()
#                     parsed_data = json.loads(mock_data)
#                     symbol = parsed_data.get("symbol")
#
#                     if symbol and symbol not in symbol_data_dict:
#                         symbol_data_dict[symbol] = parsed_data
#
#                     data_queue.task_done()
#                 except asyncio.QueueEmpty:
#                     print("Queue is empty, waiting for data...")
#
#             # 수집된 데이터를 전송
#             bundled_data = json.dumps(list(symbol_data_dict.values()))
#             print(f"Sending bundled data: {bundled_data}")
#             yield f"data: {bundled_data}\n\n"
#
#             # 데이터 초기화
#             symbol_data_dict.clear()
#             await asyncio.sleep(1)
#
#     return StreamingResponse(event_generator(), media_type="text/event-stream")