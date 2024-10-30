from fastapi import HTTPException
from src.database import get_db_connection


# 특정 심볼로 회사 정보 조회
def get_company_by_symbol(symbol: str):
    database = get_db_connection()
    cursor = database.cursor(dictionary=True)

    cursor.execute("SELECT id, name, symbol FROM company WHERE symbol = %s AND is_deleted = 0", (symbol,))
    company = cursor.fetchone()
    cursor.close()
    database.close()

    if not company:
        raise HTTPException(status_code=404, detail="회사를 찾을 수 없습니다.")

    return company