from fastapi import FastAPI, HTTPException, APIRouter
from starlette.middleware.cors import CORSMiddleware

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

@app.get("/")
def hello():
    return {"message": "메인페이지입니다"}
