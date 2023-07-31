from pydantic import BaseModel
from typing import Optional,List
class RutaEnActivo(BaseModel):
    Pos: str
    Codigo_pedido: str
    Comuna: str
    SKU: str
    Producto: str
    Unidades: int
    Bultos: int
    Nombre_cliente: Optional[str]
    Direccion_cliente: Optional[str]
    Telefono: Optional[str]
    Validacion: Optional[str]
    DE: Optional[str]
    DP: Optional[str]
    arrayProductos : List[str]
    arraySKU : List[str]