from pydantic import BaseModel
from typing import Optional

class ProductoSinClasificacion(BaseModel):
    SKU: str
    Descripcion: str
    Talla: str
    Origen: Optional[str]


