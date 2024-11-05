from fastapi import APIRouter, HTTPException, Query
from .crud import get_company_by_symbol
from .schemas import CompanyResponse
from .websocket import run_websocket_background_multiple, run_mock_websocket_background_multiple
from .crud import get_symbols_for_page
from fastapi.responses import StreamingResponse
import json
import asyncio

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


@router.get("/stream/multiple")
async def sse_stream_multiple(page: int = Query(1)):
    # 해당 페이지의 심볼 목록을 가져옴
    symbols = [{"symbol": symbol} for symbol in get_symbols_for_page(page)]

    # SSE 이벤트 생성기 - 모든 심볼 데이터를 묶어 전송
    async def event_generator():
        data_queue = await run_websocket_background_multiple(symbols)  # Mock WebSocket 데이터 수집

        while True:
            symbol_data_list = []  # 여러 심볼 데이터를 한 번에 저장할 리스트

            # 설정된 개수의 심볼 데이터 수집
            for _ in range(len(symbols)):
                mock_data = await data_queue.get()  # Queue에서 개별 데이터를 가져옴
                symbol_data_list.append(json.loads(mock_data))  # JSON으로 파싱하여 리스트에 추가
                data_queue.task_done()

            # 모든 데이터를 묶어서 한 번에 전송
            bundled_data = json.dumps(symbol_data_list)  # 리스트를 JSON 문자열로 변환
            yield f"data: {bundled_data}\n\n"

            # 원하는 시간 간격 설정 (예: 1초 간격)
            await asyncio.sleep(1)

    # StreamingResponse를 이용하여 클라이언트에 이벤트를 지속적으로 전송
    return StreamingResponse(event_generator(), media_type="text/event-stream")
