from pydantic import BaseModel


class PedidosPendientes(BaseModel):
    Atrasadas: int
    En_fecha: int
    Adelantadas: int