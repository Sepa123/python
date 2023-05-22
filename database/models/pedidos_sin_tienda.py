from pydantic import BaseModel

class PedidosSinTienda:
    suborden: int
    id_entrega: int
    descripcion: str
    unidades: int
    fecha_compromiso: str