from pydantic import BaseModel


class ReporteProducto(BaseModel):
    día: str
    fecha: str
    electrolux: int
    sportex: int
    easy: int
    easy_opl:int