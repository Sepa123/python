from pydantic import BaseModel
from datetime import date
from typing import Optional

class AsignarEntregaActa(BaseModel):
    nombres: Optional[str]
    apellidos: Optional[str]
    rut: Optional[str]
    cargo: Optional[str]
    marca: Optional[str]
    equipo: Optional[str]
    serial: Optional[str]
    fecha_entrega: Optional [date]
    # folio_entrega: Optional[str]
    encargado_entrega:Optional[str]


