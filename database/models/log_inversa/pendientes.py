from pydantic import BaseModel
from typing import Optional

class PedidosPendientes (BaseModel):
    Origen: Optional[str]
    Cod_entrega: Optional[str]
    Fecha_ingreso: Optional[str]
    Fecha_compromiso:Optional[str]
    Region: Optional[str]
    Comuna: Optional[str]
    Descripcion: Optional[str]
    Bultos: Optional[str]
    Estado: Optional[str]
    Subestado: Optional[str]
    Verificado: Optional[bool]
    Recibido: Optional[bool]