from pydantic import BaseModel
from typing import Optional
from datetime import date

class Reclutamiento(BaseModel):
    Id_reclutamiento : int
    Id_user: int
    Ids_user: Optional[str]
    Modificacion: Optional[str]
    Latitud: Optional[str]
    Longitud: Optional[str]
    Origen: Optional[str]
    Region: int
    Operacion_postula : int
    Nombre_contacto : str
    Telefono : Optional[str]
    Tipo_vehiculo : int
    Origen_contacto : int
    Estado_contacto : int
    Motivo_subestado : Optional[int]
    Contacto_ejecutivo : Optional[int]
    Razon_social : Optional[str]
    Rut_empresa : Optional[str]
    # Internalizado : Optional[bool]



class ComentarioRecluta(BaseModel):
    Id : int ## estatus_comentario
    Id_recluta: int ## id_recluta
    Id_user: int
    Ids_user: Optional[str]
    Latitud: Optional[str]
    Longitud: Optional[str]
    Comentario : Optional[str]



