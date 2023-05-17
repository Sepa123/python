from pydantic import BaseModel

class pedidos_compromiso_sin_despacho(BaseModel):
    origen: str
    cod_entrega: int
    fecha_ingreso: str
    fecha_compromiso: str
    region: str
    comuna: str
    descripcion: str
    bultos: int