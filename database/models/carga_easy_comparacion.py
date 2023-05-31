from pydantic import BaseModel
from typing import Optional


class CargaEasyComparacion(BaseModel):
    Nro_carga: str
    Cantidad: int
