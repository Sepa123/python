from pydantic import BaseModel
from datetime import datetime
from typing import Optional,List,Any 


class Notificacion(BaseModel):
    Notificacion: Optional[bool]
    Mail: str
    Perfil : Optional[int]
    # exceptionMessage: None
    # totalItems: int
    # pageNumber: int
    # itemsPerPage: int