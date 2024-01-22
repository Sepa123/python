from pydantic import BaseModel
from typing import Optional

class FirmaActa(BaseModel):
    id: int
    firma_entrega: Optional[bool]
    firma_devolucion: Optional[bool]
    estado: Optional[int]
    status: Optional[int]
    equipo_id: Optional[int]
    observacion: Optional[str]
    sub_estado:Optional[int]
    subestado:Optional[int]
    id_usuario:Optional[int]
    ids_usuario:Optional[str]
    observacion:Optional[str]
    sub_estado:Optional[int]
    lat:Optional[str]
    long:Optional[str]
    ubicacionarchivo:Optional[str]
