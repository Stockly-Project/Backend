from fastapi import APIRouter, HTTPException, Query
from .crud import get_company_by_symbol
from .schemas import CompanyResponse

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

