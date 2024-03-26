from pydantic import BaseModel
from typing import Optional

class LiberarLicencia(BaseModel):
    id_licencia:int
    id_user: int 
    ids_user: str
    lat: str
    long: str
    observacion: str
    persona:  int
    equipo: int
    asignado: bool
    status: int
