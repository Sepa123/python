from datetime import datetime
from pydantic import BaseModel
from typing import Optional


class Recepcion_tiendas(BaseModel):
    codigo_cliente: str
    nombre: str
    calle: str
    provincia: str
    codigo_pedido: str
    fecha_pedido: datetime
    codigo_producto: str
    descripcion_producto: str
    cantidad_producto: int
    sku: str
    pistoleado: bool


class bodyUpdateVerified(BaseModel):
    id_usuario: int
    cliente: Optional[str]
    n_guia: Optional[str]
    cod_pedido: str
    cod_producto : str



