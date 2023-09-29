from typing import Optional , List
from pydantic import BaseModel

class NotaVenta(Optional):
    Id_user: Optional[int]
    Ids_user: Optional[str]
    Sucursal: Optional[int]
    Cliente: Optional[str]
    Direccion: Optional[str]
    Comuna: Optional[str]
    Region: Optional[str]
    Fecha_entrega: Optional[str]
    Tipo_despacho: Optional[int]
    Numero_factura: Optional[str]
    Codigo_ty: Optional[str]
    Entregado: Optional[bool]
    arrays : List['NotaVentaProducto'] 
    # arrays : NotaVentaProducto

class NotaVentaProducto(BaseModel):
    Id_venta: Optional[int]
    Codigo: Optional[str]
    Unidades: Optional[int]
    Id_user: Optional[int]
    Ids_user: Optional[str]