from pydantic import BaseModel
from typing import Optional

class BodyEntregaNotaVenta(BaseModel):
    Id_venta : Optional[int]
    Fecha_despacho : Optional[str]
    Fecha_full_data : Optional[str]