from pydantic import BaseModel
from typing import Optional

class PedidosCompromisoSinDespacho(BaseModel):
    Origen: str
    Cod_entrega: int
    Fecha_ingreso: str
    Fecha_compromiso: str
    Region: str
    Comuna: str
    Descripcion: str
    Bultos: int