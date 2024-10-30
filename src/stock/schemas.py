from pydantic import BaseModel

class CompanyData(BaseModel):
    company_id: int
    name: str
    symbol: str

class CompanyResponse(BaseModel):
    code: int
    message: str
    data: CompanyData
