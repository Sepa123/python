from typing import Optional
from pydantic import BaseModel

class RetiroCliente(BaseModel):
    Cliente: str
    Codigo_pedido: str
    Tipo: str
    Envio_asociado: str
    Fecha_pedido: str
    SKU: str
    Descripcion_producto: str
    Cantidad: str
    Bultos: str
    Nombre_cliente: str
    Direccion: str
    Comuna: str
    Telefono: str
    Email: str
    Region: int