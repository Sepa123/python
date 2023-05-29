from pydantic import BaseModel

class PedidosCompromisoSinDespacho(BaseModel):
    Origen: str
    Cod_entrega: int
    Fecha_ingreso: str
    Fecha_compromiso: str
    Region: str
    Comuna: str
    Descripcion: str
    Bultos: int