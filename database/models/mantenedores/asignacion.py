from pydantic import BaseModel
from datetime import date
from typing import Optional

class AsignarEquipo(BaseModel):
    id_user: int 
    ids_user: str
    lat: str
    long : str
    equipo: int
    persona: Optional[int]
    fecha_entrega: Optional [date]
    estado: Optional[ bool]
    fecha_devolucion: Optional [date]
    observacion: Optional [str]
    folio_entrega: Optional[int]
    folio_devolucion: Optional [int]
    firma_entrega: Optional[bool]
    firma_devolucion: Optional[bool]
    pdf_entrega: Optional[str]
    pdf_devolucion: Optional[str]
    departamento: Optional [int]
    id_licencia: Optional[int]
    id_chip: Optional[int]
    status: Optional[int]
    subestado: Optional[int]
    sub_estado:Optional[int]
    ubicacionarchivo: Optional[str]
    estadoChip: Optional[int]
    subestadoChip: Optional[int]
    #cuando se requiere enviar el estado de equipo y asignacion en el mismo contenido se cambia el nombre de la variable
