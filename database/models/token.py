from pydantic import BaseModel
from typing import Optional

class tokenSchema(BaseModel):
    access_token:str
    refresh_token:str

class TokenPayload(BaseModel):
    sub: str
    exp: int
    email: str
    active: bool
    rol_id: int