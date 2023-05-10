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
    id: Optional[int]
    full_name: str
    mail: str
    phone: int
    position: str
    homologation: str
    active: str
    password: str
