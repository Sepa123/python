from pydantic import BaseModel
from typing import Optional

class userSchema(BaseModel):
    id: Optional[int]
    username: str
    password: str

class loginSchema(BaseModel):
    mail: str
    password: str

class User:
    id: int
    full_name: str
    mail: str
    password: str
    active: bool
    rol_id: int
