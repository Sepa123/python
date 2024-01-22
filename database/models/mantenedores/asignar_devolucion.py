from pydantic import BaseModel
from datetime import date
from typing import Optional, List


##infor es un array de objetos por lo cual se debe identificar el tipo de dato enviado en el objeto
class info(BaseModel):
    tipo: Optional[str]
    marca: Optional[str]
    serial: Optional[str]
    descripcion: Optional[str]

class AsignarDevolucionActa(BaseModel):
    id: int
    nombres: Optional[str]
    apellidos: Optional[str]
    rut: Optional[str]
    cargo: Optional[str]
    # marca: Optional[List[str]]
    info: Optional[List[info]] ## especificamos que la lista envia un objeto con el modelo indicado
    marca: Optional[str]
    equipo: Optional[str]
    serial: Optional[str]
    # serial: Optional[List[str]]
    fecha_devolucion: Optional [str]
    folio_devolucion: Optional[str]
    encargado_entrega:Optional[str]
    status:Optional[int]
    estado:Optional[bool]
    equipo_id:Optional[int]
    descripcion:Optional[str]
    almacenamiento: Optional[str]
    serial: Optional[str]
    ram: Optional[str]
    modelo: Optional[str]
    tipo:Optional[str]
    id_usuario:Optional[int]
    ids_usuario:Optional[str]
    observacion:Optional[str]
    sub_estado:Optional[int]
    lat:Optional[str]
    long:Optional[str]
    subestado:Optional[int]


