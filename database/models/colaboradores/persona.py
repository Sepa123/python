from pydantic import BaseModel
from typing import Optional


class Usuario(BaseModel):
    Rut_razon_social: Optional[str] = None
    Nombre_completo_razon_social: Optional[str] = None
    Created_at: Optional[str] = None
    Id_ingreso_hela: Optional[int] = None
    Id_user: Optional[int] = None
    Ids_user: Optional[str] = None
    Id_razon_social: Optional[int] = None
    Jpg_foto_perfil: Optional[str] = None
    Nombre_completo: Optional[str] = None
    Rut: Optional[str] = None
    Nro_serie_cedula: Optional[str] = None
    Email: Optional[str] = None
    Telefono: Optional[str] = None
    Birthday: Optional[str] = None
    Region: Optional[str] = None
    Comuna: Optional[str] = None
    Domicilio: Optional[str] = None
    Tipo_usuario: Optional[int] = None
    Pdf_antecedentes: Optional[str] = None
    Pdf_licencia_conducir: Optional[str] = None
    Fec_venc_lic_conducir: Optional[str] = None
    Pdf_cedula_identidad: Optional[str] = None
    Pdf_contrato: Optional[str] = None
    Activo: Optional[bool] = None
    Validacion_seguridad: Optional[int] = None
    Validacion_transporte: Optional[int] = None