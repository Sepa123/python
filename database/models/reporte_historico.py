from pydantic import BaseModel


class ReporteHistorico(BaseModel):
    día: str
    fecha: str
    electrolux: int
    sportex: int
    easy: int
    easy_opl: int

