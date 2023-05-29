from pydantic import BaseModel

class Pedidos(BaseModel):
    Total_pedidos: int
    Entregados: int
    No_entregados: int
    Pendientes: int