from pydantic import BaseModel
from typing import Optional

class PedidosPendientes(BaseModel):
    Atrasadas: int
    En_fecha: int
    Adelantadas: int