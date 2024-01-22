from pydantic import BaseModel
from typing import Optional

class EstadoInventario(BaseModel):
    id: Optional[int]
    estado: int
    descripcion: Optional [str]