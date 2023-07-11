from pydantic import BaseModel
from typing import Optional

class Usuario(BaseModel):
    Nombre: str
    Mail: str
    Password: str
    Activate: bool
    Rol: str