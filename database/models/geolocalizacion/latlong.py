from typing import Optional
from pydantic import BaseModel

class Latlong (BaseModel):
    Id_usuario: Optional[int]
    Direccion: Optional[str]
    Comuna: Optional[str]
    Region: Optional[str]
    Lat: Optional[str]
    Lng: Optional[str]
    Ids_usuario: Optional[str]
    Display_name: Optional[str]
    Type: Optional[str]