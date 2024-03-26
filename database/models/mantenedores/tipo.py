from pydantic import BaseModel
from typing import Optional

class TipoDeEquipo(BaseModel):
    id: Optional[int]
    nombre: str
    documentacion: bool
