import pytest
from fastapi.testclient import TestClient
from src.main import app  # main.py 파일이 있는 위치에 따라 경로를 수정하세요.

client = TestClient(app)

def test_read_main():
    response = client.get("/")
    assert response.status_code == 200  # 상태 코드가 200인지 확인
    assert response.json() == {"message": "메인페이지입니다"}  # JSON 응답이 예상값과 일치하는지 확인
