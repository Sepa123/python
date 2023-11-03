from pydantic import BaseModel
from typing import Optional

class TipoDeEquipo(BaseModel):
    id: int 
    nombre: str
