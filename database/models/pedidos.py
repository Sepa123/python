from pydantic import BaseModel

class Pedidos(BaseModel):
    total_pedidos: int
    entregados: int
    no_entregados: int
    pendientes: int