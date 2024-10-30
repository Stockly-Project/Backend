from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse
from .websocket import run_websocket_background, start_mock_websocket
from .crud import get_company_by_symbol
from .schemas import CompanyResponse
import json

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



@router.get("/info", response_model=CompanyResponse)
async def get_company_info(symbol: str):
    company = get_company_by_symbol(symbol=symbol)
    if not company:
        raise HTTPException(status_code=404, detail="회사를 찾을 수 없습니다.")

    # ensure_ascii=False를 적용하여 한글이 깨지지 않도록 설정
    content = {
        "code": 200,
        "message": "회사 정보를 조회했습니다.",
        "data": {
            "company_id": company["id"],
            "name": company["name"],
            "symbol": company["symbol"]
        }
    }

    return JSONResponse(content=content, media_type="application/json; charset=utf-8")