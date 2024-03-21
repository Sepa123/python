from pydantic import BaseModel
from typing import Optional
from datetime import date

class LiberarInsumo(BaseModel):
    id:int
    id_user: int 
    ids_user: str   
    lat: str
    long: str
    observacion: Optional[str]
    estadoInsumo: int
    subestadoInsumo:int
    fecha_devolucion: date
    folio_devolucion: int
    status: bool
    equipo: Optional[int]
