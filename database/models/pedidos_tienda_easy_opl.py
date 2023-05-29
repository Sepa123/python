from pydantic import BaseModel

class PedidosTiendaEasyOPL(BaseModel):
    Tienda: str
    Productos: int