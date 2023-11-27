from pydantic import BaseModel
from typing import Optional


class ArmarVenta (BaseModel):
    Nota_venta : Optional[int]
    Sucursal : Optional[int]