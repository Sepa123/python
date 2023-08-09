from typing import Optional
from pydantic import BaseModel

class Latlong (BaseModel):
    id_usuario: int
    direccion: Optional[str]
    comuna: Optional[str]
    region: Optional[str]
    lat: Optional[str]
    lng: Optional[str]
    ids_usuario: str
    display_name: Optional[str]
    type: Optional[str]