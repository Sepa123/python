from pydantic import BaseModel
from typing import Optional

class CentroOperacion(BaseModel):
    Id_user: Optional[int]
    Ids_user: Optional[str]
    Id_op: Optional[str]
    Centro: Optional[str]
    Descripcion: Optional[str]
    Region: Optional[int]
