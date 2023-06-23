from pydantic import BaseModel
from typing import Optional

class RutasAsignadas(BaseModel):
    asigned_by: str
    id_ruta: str
    nombre_ruta: str
    patente: str
    driver: str
    cantidad_producto: int
    estado: Optional[str]
    region: str
