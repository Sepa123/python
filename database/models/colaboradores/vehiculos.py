from typing import Optional
from pydantic import BaseModel

class Vehiculos(BaseModel):
    Razon_id: Optional[int]
    Ppu: Optional[str]
    
    Tipo: Optional[int]
    Caracteristica: Optional[int]
    Marca: Optional[int]
    Modelo: Optional[str]
    Ano: Optional[int]
    Rut_colaborador: Optional[str]
    Region: Optional[int]
    Comuna: Optional[int]

    Estado: Optional[bool]
    Activation_date: Optional[str]
    Capacidad_carga_kg: Optional[int]
    Capacidad_carga_m3: Optional[int]
    Platform_load_capacity_kg: Optional[int]
    Crane_load_capacity_kg: Optional[int]
    Permiso_circulacion_fec_venc: Optional[str]
    Soap_fec_venc: Optional[str]
    Revision_tecnica_fec_venc: Optional[str]
    Agency_id: Optional[int]
    Registration_certificate: Optional[str]
    Pdf_revision_tecnica: Optional[str]
    Pdf_soap: Optional[str]
    Pdf_padron: Optional[str]
    Pdf_gases_certification: Optional[str]
    Id_user: Optional[int]
    Ids_user: Optional[str]
    Hab_vehiculo : Optional[bool]
    Hab_seguridad : Optional[bool]
    Id_gps : Optional[int]
    Gps : Optional[bool]
    Imei :Optional[str]
    Oc_instalacion : Optional[str]
    Fecha_instalacion : Optional[str]


class AsignarOperacion(BaseModel):
    Id_ppu : Optional[int]
    Id_operacion : Optional[int]
    Id_centro : Optional[int]
    Id_user: Optional[int]
    Ids_user: Optional[str]
    Estado :  Optional[bool]