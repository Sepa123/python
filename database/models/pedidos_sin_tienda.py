from pydantic import BaseModel
from typing import Optional

class PedidosSinTienda:
    Suborden: int
    Id_entrega: int
    Descripcion: str
    Unidades: int
    Fecha_compromiso: str