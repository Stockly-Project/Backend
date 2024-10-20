from fastapi import FastAPI, HTTPException, APIRouter
from starlette.middleware.cors import CORSMiddleware
import mysql.connector
from src.configs import HOST, USER, PASSWORD, DATABASE
from src.user import routes as user_routes
app = FastAPI()
router = APIRouter(prefix="/api/v1")
app.include_router(user_routes.router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MySQL 데이터베이스에 연결
def get_db_connection():
    connection = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )
    return connection
@app.get("/")
def hello():
    return {"message": "메인페이지입니다"}
