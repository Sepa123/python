from pydantic import BaseModel
from typing import Optional

class TipoDeEquipo(BaseModel):
    nombre: str
    descripcion: Optional [str]