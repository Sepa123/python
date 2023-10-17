from pydantic import BaseModel
from typing import List , Optional

class BodySumaEstructura (BaseModel):
    Derecha : Optional[List[str]]
    Izquierda : Optional[List[str]]
    Estructura :str
    Sucursal : str