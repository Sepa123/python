from pydantic import BaseModel
from datetime import date
from typing import Optional

class ReporteProducto(BaseModel):
    Dia: str
    Fecha: date
    Electrolux: int
    Sportex: int
    Easy: int
    Easy_OPL: int