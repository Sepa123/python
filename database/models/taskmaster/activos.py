from typing import Optional
from pydantic import BaseModel

class Activo(BaseModel):
    Id: Optional[int]
    Created_at: Optional[str]  # Can be replaced with `datetime` if needed
    Last_update: Optional[str]  # Can be replaced with `datetime` if needed
    Id_user: int
    Id_area: Optional[int]
    Codigo_equipo: str
    Nombre_equipo: str
    Categoria: int
    Marca: Optional[str]
    Modelo: Optional[str]
    Region: Optional[int]
    Comuna: Optional[int]
    Direccion: Optional[str]
    Latitud: Optional[str]
    Longitud: Optional[str]
    Descripcion: Optional[str]
    Imagen_1: Optional[str]
    Imagen_2: Optional[str]
    Imagen_3: Optional[str]
    Manual_pdf: Optional[str]
    Activo: Optional[bool]