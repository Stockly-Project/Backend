from pydantic import BaseModel, EmailStr, Field, field_validator
from fastapi import HTTPException

class UserCreateDTO(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)  # 최소 1자, 최대 100자
    email: EmailStr  # 이메일 형식으로 유효성 검증
    password: str  # 비밀번호 검증은 커스텀 밸리데이터로 처리

    @field_validator('password')
    def validate_password(cls, password):
        if len(password) < 8:
            raise HTTPException(status_code=400, detail="비밀번호는 최소 8자 이상이어야 합니다.")
        if not any(char.isdigit() for char in password):
            raise HTTPException(status_code=400, detail="비밀번호에는 숫자가 포함되어야 합니다.")
        if not any(char.isalpha() for char in password):
            raise HTTPException(status_code=400, detail="비밀번호에는 영문자가 포함되어야 합니다.")
        if not any(char in "@$!%*?&" for char in password):
            raise HTTPException(status_code=400, detail="비밀번호에는 특수 문자가 포함되어야 합니다.")
        return password

    class Config:
        from_attributes = True

# 사용자 정보 조회 응답 DTO
class UserResponseDTO(BaseModel):
    userId: int
    email: EmailStr
    name: str

    class Config:
        from_attributes = True

class UserLoginDTO(BaseModel):
    email: EmailStr
    password: str