from pydantic import BaseModel
from typing import Optional


class SubEstadoInventario(BaseModel):
    id: Optional[int]
    parent_code: int
    code: int
    descripcion: str