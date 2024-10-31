from fastapi import HTTPException
from src.database import get_db_connection
from typing import List, Dict

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

def get_symbols_for_page(page: int, page_size: int = 20) -> List[Dict[str, str]]:
    start_index = (page - 1) * page_size
    database = get_db_connection()
    cursor = database.cursor()

    query = """
        SELECT id, name, symbol
        FROM company
        WHERE is_deleted = 0
        ORDER BY id
        LIMIT %s OFFSET %s
    """
    cursor.execute(query, (page_size, start_index))
    symbols = [{"id": row[0], "name": row[1], "symbol": row[2]} for row in cursor.fetchall()]

    cursor.close()
    database.close()

    return symbols