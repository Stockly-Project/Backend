from fastapi import APIRouter, Request, Depends, HTTPException
from .schemas import NotificationSchema 
from .crud import create_notification_prices
from src.database import get_redis
from src.user.crud import get_authenticated_user_from_session_id, get_user_from_session, get_notification_prices,delete_notification_prices,get_notification_messages, delete_notification_messages

router = APIRouter(
    prefix="/api/v1/alert",
    tags=["alerts"],
)

# 알림 조건 설정 엔드포인트 
@router.post('/prices')
async def create_notification_prices_endpoint(
    request: Request, 
    notification_data: NotificationSchema, 
    redis=Depends(get_redis)
):
    user_id = await get_authenticated_user_from_session_id(request, redis)
    notification_data_with_user = notification_data.dict()
    notification_data_with_user["user_id"] = user_id
    result = await create_notification_prices(notification_data_with_user)
    return result


# 알림 조건 목록 조회 엔드포인트(is_active와 is_deleted가 0인 경우만 조회)
@router.get('/prices')
async def get_prices(request: Request, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    user_id = await get_user_from_session(session_id,redis)
    result = await get_notification_prices(user_id)
    return result

# 알림 조건 목록 삭제 엔드포인트(is_active가 0인 경우에, is_deleted를 1로 업데이트)
@router.delete('/prices/{notification_id}')
async def delete_prices(request: Request, notification_id: int, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    user_id = await get_user_from_session(session_id,redis)
    result = await delete_notification_prices(user_id, notification_id)
    return result

# 알림 메시지 조회 엔드포인트(is_active가 1이고, is_deleted가 0인 경우에만 조회)
@router.get('/messages')
async def get_messages(request: Request, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    user_id = await get_user_from_session(session_id,redis)
    result = await get_notification_messages(user_id)
    return result

# 알림 메시지 삭제 엔드₩포인트(is_active가 1인 경우에, is_deleted를 1로 업데이트)
@router.delete('/messages/{notification_id}')
async def delete_messages(request: Request, notification_id: int, redis=Depends(get_redis)):
    session_id = request.cookies.get("session_id")
    if not session_id:
        raise HTTPException(status_code=401, detail="세션 ID가 없습니다.")
    user_id = await get_user_from_session(session_id,redis)
    result = await delete_notification_messages(user_id, notification_id)
    return result
