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
    sku: Optional[str]
    cod_pedido: str
    cod_producto : str
    ids_usuario : Optional[str]


class dataBitacora(BaseModel):
    id: int
    created_at: datetime
    user_id: int
    origen: str
    cliente: str
    componentes: str
    descripcion: str
    imagen1: str
    imagen2: str
    imagen3: str
    tipo_dano: int
    momento: int
    lat: str
    lng: str


