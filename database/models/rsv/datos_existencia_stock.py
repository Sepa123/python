from pydantic import BaseModel
from typing import Optional

class ExistenciaStock(BaseModel):
    # %(Codigo_producto)s,%(Cantidad)s,%(Sucursal)s
    Codigo_producto : Optional[str]
    Cantidad : Optional[int]
    Sucursal : Optional[int]