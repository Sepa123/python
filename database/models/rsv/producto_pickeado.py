from pydantic import BaseModel
from typing import Optional


class RsVPickeado (BaseModel):
    # Id: int
    # Created_at: str
    Fecha_ingreso: str
    Id_user: int
    Ids_user: str
    Nombre_carga: str
    Codigo: str
    Color: int
    Paquetes: int
    Unidades: int
    Verificado: Optional[bool]
    Descripcion: str
