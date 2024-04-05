from typing import Optional
from pydantic import BaseModel


class ReingresoOperacion((BaseModel)):
    id_user: Optional[int]
    ids_user: Optional[str]
    cliente: Optional[str]
    codigo_pedido: Optional[str]
    lat: Optional[str]
    long: Optional[str]
    ingreso: Optional[str]