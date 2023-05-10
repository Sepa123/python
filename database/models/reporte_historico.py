from pydantic import BaseModel


class ReporteHistorico(BaseModel):
    día: str
    fecha: str
    electrolux: int
    sportex: int
    easy: int
    tiendas: int

    # def __init__(self, día: str, fecha: str, electrolux: int, sportex: int, easy: int, tiendas: int) -> None:
    #     self.día = día
    #     self.fecha = fecha
    #     self.electrolux = electrolux
    #     self.sportex = sportex
    #     self.easy = easy
    #     self.tiendas = tiendas


