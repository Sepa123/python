from pydantic import BaseModel
from typing import Optional


class ObtUbicacionCantidad (BaseModel):
    Codigo : Optional[str]
    Sucursal : Optional[int]