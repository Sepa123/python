from pydantic import BaseModel
from typing import Optional

class CentroOperacion(BaseModel):
    Id_user: Optional[int]
    Ids_user: Optional[str]
    Id_op: Optional[str]
    Centro: Optional[str]
    Descripcion: Optional[str]
    Region: Optional[int]
    Id_modo_seg : Optional[int]


class UpdateCentroOperacion(BaseModel):
    Id_usuario: Optional[int]
    Ids_usuario: Optional[str]
    Id_op: Optional[int]
    Id_co: Optional[int]
    Centro: Optional[str]
    Descripcion: Optional[str]
    Region: Optional[int]