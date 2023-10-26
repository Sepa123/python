from pydantic import BaseModel
from datetime import date
from typing import Optional

class AsignarEquipo(BaseModel):
    id_user: int 
    ids_user: str
    lat: str
    long : str
    equipo: int
    persona: int
    fecha_entrega: Optional [date]
    estado: Optional[ bool]
    fecha_devolucion: Optional [date]
    observacion: Optional [str]
    nombre_equipo: Optional [str]
    folio: Optional [str]
    departamento: Optional [int]
