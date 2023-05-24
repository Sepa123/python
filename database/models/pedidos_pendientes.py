from pydantic import BaseModel


class PedidosPendientes(BaseModel):
    atrasadas: int
    en_fecha: int
    adelantadas: int