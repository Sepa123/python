from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class ProductoSKU(BaseModel):
    Codigo_cliente: Optional[str]
    Nombre: Optional[str]
    Calle: str
    Provincia: str
    Codigo_pedido: str
    Fecha_pedido: datetime
    Codigo_producto: str
    Descripcion_producto: str
    Cantidad_producto: int
    SKU: int
    Pistoleado: str

