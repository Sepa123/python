from typing import Optional
from pydantic import BaseModel

class RetiroCliente(BaseModel):
    Cliente: str
    Codigo_pedido: str
    Tipo: str
    Envio_asociado: str
    Fecha_pedido: str
    SKU: int
    Descripcion_producto: str
    Cantidad: int
    Bultos: int
    Nombre_cliente: str
    Direccion: str
    Comuna: str
    Telefono: str
    Email: str
    Region: int
    Id_usuario : str