from typing import Optional
from pydantic import BaseModel


class ReingresoOperacion(BaseModel):
    Id_user: Optional[int]
    Ids_user: Optional[str]
    Cliente: Optional[str]
    Codigo_pedido: Optional[str]
    Lat: Optional[str]
    Long: Optional[str]
    Ingreso: Optional[str]