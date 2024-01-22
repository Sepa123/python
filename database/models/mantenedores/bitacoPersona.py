from pydantic import BaseModel
from typing import Optional

class BitacoraPersona(BaseModel):
    id: int
    id_user: Optional [int]
    ids_user: Optional [str]
    lat: Optional [str]
    long : Optional [str]
    observacion: Optional[str]
 

