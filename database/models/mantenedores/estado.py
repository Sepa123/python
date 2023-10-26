from pydantic import BaseModel
from typing import Optional

class EstadoInventario(BaseModel):
    id_user: Optional [int]
    ids_user: Optional [str]
    nombre: Optional [str]