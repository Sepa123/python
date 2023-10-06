from pydantic import BaseModel

class BodyEntregaNotaVenta(BaseModel):
    Id_venta : int
    Fecha_despacho : str
    Fecha_full_data : str