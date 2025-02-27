from typing import Optional
from pydantic import BaseModel
from decimal import Decimal

class Colaboradores(BaseModel):
    Id_user: Optional[int]
    Ids_user: Optional[str]
    Razon_social: Optional[str]
    Tipo_razon: Optional[int]
    Rut: Optional[str]
    Celular: Optional[str]
    Telefono: Optional[str]
    Email: Optional[str]
    Region: Optional[int]
    Comuna: Optional[int]
    Direccion: Optional[str]
    Representante_legal: Optional[str]
    Rut_representante_legal:  Optional[str]
    Email_representante_legal:  Optional[str]
    Direccion_comercial :Optional[str]
    Fecha_nacimiento:  Optional[str]
    Activo : Optional[bool]
    Giro : Optional[str]
    Chofer : Optional[bool]
    Peoneta : Optional[bool]
    Abogado : Optional[int]
    Seguridad : Optional[int]
    Modificacion: Optional[str]
    Latitud: Optional[str]
    Longitud: Optional[str]
    Origen: Optional[str]


class DetallesPago(BaseModel):
    Id_user: Optional[int]
    Ids_user: Optional[str]
    Id_razon_social: Optional[int]
    Rut_titular_cta_bancaria: Optional[str]
    Titular_cta: Optional[str]
    Numero_cta: Optional[str]
    Banco: Optional[int]
    Email: Optional[str]
    Tipo_cta: Optional[int]
    Forma_pago: Optional[int]
    Pdf_documento: Optional[str]
    Estado : Optional[int]


class DesvincularColaborador(BaseModel):
    Id_user: Optional[int] = None
    Ids_user: Optional[str] = None
    Rut: Optional[str] = None
    Id_razon_social: Optional[int] = None
    Latitud: Optional[str] = None
    Longitud: Optional[str] = None
    Descripcion_desvinculacion: Optional[str] = None
    Origen: Optional[str] = None
    Fecha_desactivacion: Optional[str] = None
    Motivo_desactivacion: Optional[str] = None
    Modificacion: Optional[str] = None
