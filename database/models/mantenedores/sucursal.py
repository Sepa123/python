from pydantic import BaseModel
from typing import Optional

class SucursalInventario(BaseModel):
    id: int
    nombre: Optional [str]
    pais: Optional [str]
    ciudad: Optional [str]
    comuna:Optional [str]
    direccion: Optional [str]
    latitud: Optional [str]
    longitud:Optional [str]
    id_usuario: Optional [str]
    ids_usuario: Optional [str]