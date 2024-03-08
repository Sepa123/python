from pydantic import BaseModel
from typing import Optional, Union

class ProductoIngresado(BaseModel):
    Ingreso_sistema: Optional[str]
    Cliente: Optional[str]
    Anden: Union[str, int, None]
    Cod_pedido: Optional[str]
    Fec_compromiso: Optional[str]
    Cod_producto: Optional[str]
    Sku: Union[str, int, None]
    Comuna: Optional[str]
    Region: Optional[str]
    Cantidad: Optional[int]
    Verificado: Optional[bool]
    Recepcionado: Optional[bool]
    Estado: Optional[str]
    Subestado: Optional[str]