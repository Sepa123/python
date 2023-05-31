from pydantic import BaseModel
from typing import Optional

class PedidosTiendaEasyOPL(BaseModel):
    Tienda: str
    Productos: int