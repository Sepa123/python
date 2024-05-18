from typing import Optional
from pydantic import BaseModel

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