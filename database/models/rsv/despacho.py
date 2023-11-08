from pydantic import BaseModel
from typing import Optional

class Despacho (BaseModel):
    Id: Optional[int]
    Id_user: int
    Ids_user: str
    Id_nota_venta: int
    Id_etiqueta: Optional[int]
    Bar_code: Optional[str]
    Lat: Optional[str]
    Lng: Optional[str]
    Cantidad: Optional[int]
    Codigo_producto : Optional[str]
    Unidades: Optional[int]
    Uni_agregadas: Optional[int]