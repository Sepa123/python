from pydantic import BaseModel
from typing import Optional

class CrearLicencia(BaseModel):
    id: Optional[int]
    id_user: Optional [int]
    ids_user: Optional [str]
    lat: Optional [str]
    long: Optional [str]
    codigo: Optional [str]