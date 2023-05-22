from pydantic import BaseModel

class PedidosTiendaEasyOPL(BaseModel):
    tienda: str
    productos: int