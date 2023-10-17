from pydantic import BaseModel
from typing import Optional


class Estructura (BaseModel):
    Nombre: str
    Sucursal: int
    Tipo: int
    Cant_espacios: int
    Balanceo: Optional[str]
    Frontis: Optional[str]
