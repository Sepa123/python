from pydantic import BaseModel
from typing import Optional


class DescripcionEquipo(BaseModel):
    id_user: Optional [int]
    ids_user: Optional [str]
    lat: Optional [str]
    long : Optional [str]
    marca: Optional [str]
    modelo: Optional [str]
    serial: Optional [str]
    mac_wifi: Optional [str]
    serie: Optional [str]
    resolucion: Optional [str]
    dimensiones: Optional [str]
    descripcion: Optional [str]
    ubicacion: Optional [str]
    almacenamiento: Optional [int]
    ram: Optional [int]
    estado: Optional [int]
    tipo: Optional [int]