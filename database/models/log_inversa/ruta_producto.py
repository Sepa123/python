from pydantic import BaseModel
from typing import Optional

class BodyRutaProducto(BaseModel):
    Separador: Optional[str]
    Nombre_ruta: Optional[str]