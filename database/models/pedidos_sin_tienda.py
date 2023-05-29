from pydantic import BaseModel

class PedidosSinTienda:
    Suborden: int
    Id_entrega: int
    Descripcion: str
    Unidades: int
    Fecha_compromiso: str