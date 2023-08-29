from pydantic import BaseModel
from typing import Optional

class RangoFecha(BaseModel):
    Fecha_inicio: Optional[str]
    Fecha_fin : Optional[str]
