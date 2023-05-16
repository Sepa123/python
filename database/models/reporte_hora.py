from pydantic import BaseModel

class reporte_hora(BaseModel):
    hora: str
    electrolux: int
    sportex: int
    easy_cd: int
    tiendas: int
    easy_opl: int