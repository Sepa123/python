from pydantic import BaseModel
from typing import Optional
from datetime import date

class BitacoraTransporte(BaseModel):
    Id_user: int
    Ids_user: Optional[str]
    Modificacion: Optional[str]
    Latitud: Optional[str]
    Longitud: Optional[str]
    Origen: Optional[str]
