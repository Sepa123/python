from pydantic import BaseModel
from typing import Optional
from datetime import date

class LiberarChip(BaseModel):
    id_chip:int
    id_user: int 
    ids_user: str
    lat: str
    long: str
    observacion: str
    persona:  int
    equipo: int
    estadoChip: int
    subestadoChip:int
    fecha_devolucion: date
    estado: bool