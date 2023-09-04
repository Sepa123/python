from pydantic import BaseModel
from typing import Optional

class EditarTOC(BaseModel):
    Guia : str
    Observacion: Optional[str]
    Subestado_esperado: Optional[str]
    Ids_transyanez: Optional[str]
    Alerta: bool
    Origen: Optional[str]
    Fecha_reprogramada: Optional[str]
    Codigo1: Optional[int]
    Codigo1Str: Optional[str]
    Direccion_correcta: Optional[str]
    Comuna_correcta: Optional[str]
    Id_usuario : str
    Ids_usuario : str
