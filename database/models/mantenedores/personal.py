from pydantic import BaseModel
from datetime import date
from typing import Optional

class PersonalEquipo(BaseModel):
    id: Optional[int]
    id_user: Optional [int]
    ids_user: Optional [str]
    lat: Optional [str]
    long : Optional [str]
    nombres: str
    apellidos: str
    rut: str
    nacionalidad: Optional[str]
    fecha_nacimiento: Optional[date]
    estado_civil: Optional [str]
    telefono: Optional [str]
    fecha_ingreso: Optional [date]
    cargo: Optional [str]
    domicilio: Optional [str]
    comuna: Optional [str]
    banco: Optional [str]
    tipo_cuenta: Optional [str]
    numero_cuenta: Optional [str]
    correo: Optional [str]
    afp: Optional [str]
    salud: Optional [str]
    telefono_adicional: Optional [str]
    nombre_contacto: Optional [str]
    seguro_covid:Optional [ bool]
    horario: Optional [str]
    ceco: Optional [str]
    sueldo_base: Optional [int]
    tipo_contrato: Optional [str]
    direccion_laboral: Optional [str]
    enfermedad: Optional [str]
    polera: Optional [str]
    pantalon: Optional [str]
    poleron: Optional [str]
    zapato: Optional [int]
    req_comp: Optional [bool]
    req_cel: Optional [bool]
    observacion: Optional[str]
    foto: Optional[str]
    pdf: Optional[str]

