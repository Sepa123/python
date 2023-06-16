from datetime import datetime
from pydantic import BaseModel

class ProductoEasyOPL(BaseModel):
    Codigo_cliente: str
    Nombre: str
    Calle: str
    # Ciudad: str
    Provincia: str
    # telefono: int
    # email: str
    Codigo_pedido: str
    Fecha_pedido: datetime
    # operacion: str
    Codigo_producto: str
    Descripcion_producto: str
    Cantidad_producto: int
    Sku: int
    Pistoleado: str
    # tamaño: str
    # estado: str
    # en_ruta: str

class bodyUpdate(BaseModel):
    cod_producto : str
    cod_sku: str