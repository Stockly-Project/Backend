from pydantic import BaseModel

class NotificationSchema(BaseModel):
    symbol: str
    price: int

    class Config:
        orm_mode = True
