from typing import Optional
from pydantic import BaseModel

class RetiroCliente(BaseModel):
    Cliente: Optional[str]
    Codigo_pedido: Optional[str]
    Tipo: Optional[str]
    Envio_asociado: Optional[str]
    Fecha_pedido: Optional[str]
    SKU: int
    Descripcion_producto: Optional[str]
    Cantidad: int
    Bultos: int
    Nombre_cliente: Optional[str]
    Direccion: Optional[str]
    Comuna: Optional[str]
    Telefono: Optional[str]
    Email: Optional[str]
    Region: Optional[int]
    Id_usuario : Optional[str]