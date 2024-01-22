from pydantic import BaseModel
from typing import Optional

class PersonaHabilitada(BaseModel):
    id: int
    observacion: Optional[str]
    id_user:Optional[int]
    ids_user:Optional[str]
    lat:Optional[str]
    long:Optional[str]
    habilitado: bool
