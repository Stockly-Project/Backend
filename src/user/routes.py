
from fastapi import APIRouter, Response, Request, HTTPException, Depends
from .schemas import UserCreateDTO, UserResponseDTO, UserLoginDTO
from .crud import create_user, get_user_by_email, soft_delete_user_by_session, get_authenticated_user_from_session_id, get_user_info_by_session
from passlib.hash import bcrypt
import uuid
from src.database import get_redis

router = APIRouter(
    prefix="/api/v1/users",
    tags=["users"],
)

# 회원가입 엔드포인트
@router.post('/signup')
async def signup(userdata: UserCreateDTO):
    create_user(userdata)
    return {"message": "회원가입이 완료되었습니다."}

# 로그인 엔드포인트 (세션 ID를 쿠키에 저장하고 Redis에 저장)
@router.post('/login')
async def login(response: Response, userdata: UserLoginDTO, redis=Depends(get_redis)):
    user = get_user_by_email(userdata.email)

    # 비밀번호 확인
    if not bcrypt.verify(userdata.password, user['password']):
        raise HTTPException(status_code=400, detail="비밀번호가 일치하지 않습니다.")

    # 세션 ID 생성
    session_id = str(uuid.uuid4())

    # Redis에 세션 ID 저장 (유효 시간 설정: 1시간)
    await redis.set(session_id, user['id'], ex=3600)

    response.set_cookie(
    key="session_id",
    value=session_id,
    httponly=True,       # JavaScript 접근 금지
    samesite="Lax",      # CSRF 방어 (Strict 대신 Lax 사용)
    max_age=3600,        # 유효 시간 설정 (초 단위)
    path="/"             # 전체 경로에서 쿠키 접근 가능
)

    return {"message": "로그인 성공", "session_id": session_id}

# 로그아웃 엔드포인트 (세션 쿠키 삭제)
@router.post('/logout')
async def logout(response: Response):
    response.delete_cookie(key="session_id")
    return {"message": "로그아웃 완료"}

# 유저 소프트 딜리트 엔드포인트 (세션 기반)
@router.delete('')
async def delete_user(request: Request, redis=Depends(get_redis)):
    # 세션 ID에서 사용자 정보 가져오기
    user_id = await get_authenticated_user_from_session_id(request,redis)
    # 유저 삭제 처리
    deleteUser = soft_delete_user_by_session(user_id)
    return deleteUser

# 유저 정보 조회 (세션 기반)
@router.get('', response_model=UserResponseDTO)
async def get_user_info(request: Request, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")

    # 세션 ID로 유저 정보 조회
    user_info = await get_user_info_by_session(session_id, redis)
    return user_info
