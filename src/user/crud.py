from fastapi import HTTPException, Request, Response, Depends
from passlib.hash import bcrypt
from src.database import get_db_connection, get_redis
import uuid
from .schemas import UserResponseDTO
from src.logger import logger

# 이메일로 사용자 조회
def get_user_by_email(email: str):
    database = get_db_connection()
    cursor = database.cursor(dictionary=True)

    cursor.execute("SELECT id, name, email, password FROM user WHERE email = %s", (email,))
    user = cursor.fetchone()
    cursor.close()
    database.close()

    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")

    return user


# 비밀번호 해싱 및 사용자 생성
def create_user(UserCreateDTO):
    database = get_db_connection()
    cursor = database.cursor()

    # 이메일 중복 처리
    cursor.execute("SELECT id FROM user WHERE email = %s", (UserCreateDTO.email,))
    if cursor.fetchone():
        raise HTTPException(status_code=400, detail="이미 존재하는 이메일입니다.")

    # 비밀번호 해싱
    hashed_password = bcrypt.hash(UserCreateDTO.password)

    # 사용자 정보 저장
    cursor.execute(
        "INSERT INTO user (name, email, password) VALUES (%s, %s, %s)",
        (UserCreateDTO.name, UserCreateDTO.email, hashed_password)
    )
    database.commit()
    cursor.close()
    database.close()


# Redis에서 세션 ID로 사용자 조회
async def get_user_from_session(session_id: str, redis):
    user_id_bytes = await redis.get(session_id)

    if user_id_bytes is None:
        raise HTTPException(status_code=403, detail="세션이 만료되었거나 유효하지 않습니다.")

    # 사용자 ID를 bytes에서 문자열로 변환 후 int로 변환
    user_id = int(user_id_bytes.decode('utf-8'))
    return int(user_id)


# 소프트 딜리트 처리
def soft_delete_user_by_session(user_id: int):
    database = get_db_connection()
    cursor = database.cursor()

    cursor.execute("UPDATE user SET is_deleted = 1 WHERE id = %s", (user_id,))
    database.commit()
    cursor.close()
    database.close()

    return {"message": "유저가 소프트 딜리트되었습니다."}


# 세션 ID를 쿠키에서 가져와 인증하는 미들웨어
async def get_authenticated_user_from_session_id(request: Request, redis):
    """
    세션 ID를 쿠키에서 가져와 Redis를 통해 사용자 인증.
    """
    session_id = request.cookies.get("session_id")

    if not session_id:
        raise HTTPException(status_code=401, detail="Session ID is missing. Please login.")

    # 세션 ID를 통해 유저 정보 조회
    user_id = await get_user_from_session(session_id, redis)
    if not user_id:
        raise HTTPException(status_code=401, detail="Session is invalid or expired.")

    return user_id



# 세션 ID를 쿠키에서 조회하여 반환하는 함수
def get_session_id(request: Request):
    session_id = request.cookies.get("session_id")

    if session_id is None:
        raise HTTPException(status_code=401, detail="유효하지 않은 세션 ID입니다.")

    return session_id

# Redis에서 세션 ID를 저장하고 만료 시간 설정 (예: 1시간)
async def create_session(response: Response, user_id: int, redis):
    session_id = str(uuid.uuid4())  # 세션 ID 생성
    await redis.set(session_id, user_id, ex=3600)  # Redis에 세션 저장 (유효시간: 1시간)

    # 세션 ID를 쿠키에 저장
    response.set_cookie(key="session_id", value=session_id, httponly=True)
    return session_id

# 세션 ID로 유저 정보 조회 및 UserResponseDTO 반환
async def get_user_info_by_session(session_id: str, redis):
    user_id = await get_user_from_session(session_id, redis)

    # 유저 ID로 유저 정보 조회
    database = get_db_connection()
    cursor = database.cursor(dictionary=True)
    cursor.execute("SELECT id, name, email FROM user WHERE id = %s", (user_id,))
    user = cursor.fetchone()
    cursor.close()
    database.close()

    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")

    # UserResponseDTO 형식으로 반환
    return UserResponseDTO(
        userId=user['id'],
        email=user['email'],
        name=user['name']
    )


async def get_notification_prices(user_id):
    """
    특정 사용자의 알림 조건 목록 조회 (is_active=0, is_deleted=0).
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # 조건 조회 쿼리
        query = """
        SELECT n.id, c.name AS company_name, c.symbol, n.price, n.is_active
        FROM notification n
        JOIN company c ON n.company_id = c.id
        WHERE n.user_id = %s AND n.is_active = FALSE AND n.is_deleted = FALSE
        """
        cursor.execute(query, (user_id,))
        notifications = cursor.fetchall()

        # 결과 반환
        result = [
            {
                "notification_id": row[0],
                "company_name": row[1],
                "symbol": row[2],
                "price": row[3],
                "is_active": bool(row[4])
            }
            for row in notifications
        ]
        return result

    except Exception as e:
        logger.error(f"Failed to retrieve notifications: {e}")
        raise e
    finally:
        if connection:
            cursor.close()
            connection.close()

async def delete_notification_prices(user_id, notification_id):
    """
    특정 사용자의 알림 조건 삭제 (is_active=0인 경우만 삭제 가능).
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # 조건 확인 쿼리
        select_query = """
        SELECT is_active FROM notification WHERE id = %s AND user_id = %s
        """
        cursor.execute(select_query, (notification_id, user_id))
        result = cursor.fetchone()

        if not result:
            raise ValueError("Notification not found or does not belong to the user")
        if result[0]:
            raise ValueError("Cannot delete an active notification")

        # 삭제 업데이트 쿼리
        update_query = """
        UPDATE notification SET is_deleted = TRUE WHERE id = %s AND user_id = %s
        """
        cursor.execute(update_query, (notification_id, user_id))
        connection.commit()

        return {"code": 200, "message": "알림 조건 삭제 성공"}

    except Exception as e:
        logger.error(f"Failed to delete notification: {e}")
        raise e
    finally:
        if connection:
            cursor.close()
            connection.close()

async def get_notification_messages(user_id):
    """
    특정 사용자의 알림 메시지 조회 (is_active=1, is_deleted=0).
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # 알림 메시지 조회 쿼리
        query = """
        SELECT n.id, c.name AS company_name, c.symbol, n.price, n.is_active, n.updated_at
        FROM notification n
        JOIN company c ON n.company_id = c.id
        WHERE n.user_id = %s AND n.is_active = TRUE AND n.is_deleted = FALSE
        """
        cursor.execute(query, (user_id,))
        messages = cursor.fetchall()

        # 결과 반환
        result = [
            {
                "notification_id": row[0],
                "company_name": row[1],
                "symbol": row[2],
                "price": row[3],
                "is_active": bool(row[4]),
                "date": row[5].strftime('%Y-%m-%d %H:%M:%S') 
            }
            for row in messages
        ]
        return result

    except Exception as e:
        logger.error(f"Failed to retrieve notification messages: {e}")
        raise e
    finally:
        if connection:
            cursor.close()
            connection.close()


async def delete_notification_messages(user_id, notification_id):
    """
    특정 사용자의 알림 메시지 삭제 (is_active=1인 경우만 삭제 가능).
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        # 메시지 확인 쿼리
        select_query = """
        SELECT is_active FROM notification WHERE id = %s AND user_id = %s
        """
        cursor.execute(select_query, (notification_id, user_id))
        result = cursor.fetchone()

        if not result:
            raise ValueError("Notification not found or does not belong to the user")
        if not result[0]:
            raise ValueError("Cannot delete an inactive notification")

        # 삭제 업데이트 쿼리
        update_query = """
        UPDATE notification SET is_deleted = TRUE WHERE id = %s AND user_id = %s
        """
        cursor.execute(update_query, (notification_id, user_id))
        connection.commit()

        return {"code": 200, "message": "알림 메시지 삭제 성공"}

    except Exception as e:
        logger.error(f"Failed to delete notification message: {e}")
        raise e
    finally:
        if connection:
            cursor.close()
            connection.close()
