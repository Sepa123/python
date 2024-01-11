from pydantic import BaseModel
from typing import Optional
from datetime import date

class BitacoraLg(BaseModel):
    Id_user: str
    Ids_user: Optional[str]
    Estado_inicial: Optional[int]
    Subestado_inicial: Optional[int]
    Estado_final: Optional[int]
    Subestado_final: Optional[int]
    Link: Optional[str]
    Observacion: Optional[str]
    Latitud: Optional[str]
    Longitud: Optional[str]
    Origen: Optional[str]
    Codigo_pedido: Optional[str]
    Codigo_producto: Optional[str]
    Origen_registro: Optional[str]