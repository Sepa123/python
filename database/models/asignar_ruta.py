from pydantic import BaseModel
from typing import Optional

class RutasAsignadas(BaseModel):
    asigned_by: Optional[str]
    id_ruta: Optional[str]
    nombre_ruta: Optional[str]
    patente: str
    conductor: str
    cantidad_producto: int
    estado: Optional[str]
    despachador : Optional[str]
    region: str
