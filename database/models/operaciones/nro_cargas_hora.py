from pydantic import BaseModel
from datetime import date

class NroCargasHora(BaseModel):
    Hora: str
    Nro_carga: str
    Entregas: int
    Bultos: int
    Verificados: int
    No_verificados: int