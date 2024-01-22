from pydantic import BaseModel
from typing import Optional

class BitacoraEquipo(BaseModel):
    id: int
    id_user: Optional [int]
    ids_user: Optional [str]
    lat: Optional [str]
    long : Optional [str]
    observacion: Optional[str]
    status: Optional[int]
    subestado:Optional[int]
    ubicacionarchivo:Optional[int]
    estado: Optional[int]
 

