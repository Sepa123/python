from typing import Optional
from pydantic import BaseModel

class RazonSocial(BaseModel):
    id_user: int
    ids_user: str
    nombre: str
    description: str
    creation_date: str
    update_date: str
    estado: bool 
    id_mod: Optional[int]

class updateApp(BaseModel):
    id: int
    estado: bool