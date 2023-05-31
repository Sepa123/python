from pydantic import BaseModel
from typing import Optional

class ReporteHora(BaseModel):
    Hora: str
    Electrolux: int
    Sportex: int
    Easy_CD: int
    Easy_OPL: int