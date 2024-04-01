from pydantic import BaseModel
from typing import Optional


class CambioEtiqueta (BaseModel):
    # Id: Optional[int]
    Id_user: Optional[int]
    Ids_user: Optional[str]
    Id_nota_venta: Optional[int]
    Id_etiqueta: Optional[int]
    Bar_code_antiguo: Optional[str]
    Bar_code_nuevo: Optional[str]
    Cantidad: Optional[int]
    Codigo_producto : Optional[str]
    Nota_venta : Optional[int]
    Sucursal : Optional[int]