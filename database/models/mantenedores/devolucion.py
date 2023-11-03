from pydantic import BaseModel
from datetime import date
from typing import Optional

class DevolucionEquipo(BaseModel):
    id: int
    equipo: int
    persona: int
    fecha_entrega: Optional [date]
    estado: Optional[ bool]
    fecha_devolucion: Optional [date]
    observacion: Optional [str]
    nombre_equipo: Optional [str]
    folio: Optional [str]
    departamento: Optional [int]