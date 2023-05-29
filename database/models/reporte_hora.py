from pydantic import BaseModel

class ReporteHora(BaseModel):
    Hora: str
    Electrolux: int
    Sportex: int
    Easy_CD: int
    Easy_OPL: int