from pydantic import BaseModel
from typing import Optional

class Pedidos(BaseModel):
    Total_pedidos: int
    Entregados: int
    No_entregados: int
    Pendientes: int