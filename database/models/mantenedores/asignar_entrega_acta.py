from pydantic import BaseModel
from datetime import date
from typing import Optional, List
##infor es un array de objetos por lo cual se debe identificar el tipo de dato enviado en el objeto
class info(BaseModel):
    tipo: Optional[str]
    marca: Optional[str]
    serial: Optional[str]
    descripcion: Optional[str]

class AsignarEntregaActa(BaseModel):
    id: int
    nombres: Optional[str]
    apellidos: Optional[str]
    rut: Optional[str]
    cargo: Optional[str]
    info: Optional[List[info]] ## especificamos que la lista envia un objeto con el modelo indicado
    marca: Optional[str]
    equipo: Optional[str]
    serial: Optional[str]
    descripcion: Optional[str]
    fecha_entrega: Optional [str]
    folio_entrega: Optional[str]
    encargado_entrega:Optional[str]
    #columna tabla equipo
    status:Optional[int]
    ##columna tabla asignacion
    estado:Optional[bool]
    equipo_id: Optional[int]
    ##columna tabla equipo
    subestado: Optional[int]
    ##columna tabla asignacion
    sub_estado: Optional[int]
    id_usuario:Optional[int]
    ids_usuario:Optional[str]
    observacion:Optional[str]
    sub_estado:Optional[int]
    lat:Optional[str]
    long:Optional[str]




