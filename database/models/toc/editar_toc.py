from pydantic import BaseModel
from typing import Optional

class EditarTOC(BaseModel):
    Observacion: Optional[str]
    Subestado_esperado: Optional[str]
    Ids_transyanez: Optional[str]
    Alerta: bool
    # Codigo1: Optional[int]

