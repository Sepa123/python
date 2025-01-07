from typing import List, Optional, Union
from pydantic import BaseModel , validator
from decimal import Decimal


class Vehiculos(BaseModel):
    Razon_id: Optional[int]
    Ppu: Optional[str]
    
    Tipo: Optional[int]
    Caracteristica: Optional[int]
    Marca: Optional[Union[int, str]]
    Modelo: Optional[str]
    Ano: Optional[Union[int, str]]
    Rut_colaborador: Optional[str]
    Region: Optional[int]
    Comuna: Optional[int]

    Estado: Optional[bool]
    Activation_date: Optional[str]
    Capacidad_carga_kg: Optional[Union[int, str]]
    Capacidad_carga_m3: Optional[Decimal]
    Platform_load_capacity_kg: Optional[Union[int, str]]
    Crane_load_capacity_kg: Optional[Union[int, str]]
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
    Id_gps : Optional[Union[int, str]]
    Gps : Optional[bool]
    Imei :Optional[str]
    Oc_instalacion : Optional[str]
    Fecha_instalacion : Optional[str]
    Desc_desabilitado: Optional[str]
    Fecha_desinstalacion : Optional[str]
    Oc_desinstalacion: Optional[str]
    Disponible : Optional[bool]
    Habilitado : Optional[bool]
    Modificacion: Optional[str]
    Latitud: Optional[str]
    Longitud: Optional[str]
    Origen: Optional[str]

    @validator('Capacidad_carga_m3')
    def validate_numeric_fields(cls, v):
        if v is not None:
            if v < Decimal('0.00') or v > Decimal('99999999.99'):
                raise ValueError('El valor debe estar entre 0.00 y 99999999.99')
            if v.as_tuple().exponent < -2:  # Asegura que solo haya dos decimales
                raise ValueError('El valor puede tener un máximo de 2 decimales')
        return v



class AsignarOperacion(BaseModel):
    Id_ppu : Optional[int]
    Id_operacion : Optional[int]
    Id_centro : Optional[int]
    Id_user: Optional[int]
    Ids_user: Optional[str]
    Estado :  Optional[bool]

class cambiarEstadoVehiculo(BaseModel):
    id : Optional[int]
    ppu : Optional[str]
    Id_user : Optional[int]
    Ids_user : Optional[str]
    Id_ppu : Optional[str]
    Latitud : Optional[str]
    Longitud : Optional[str]
    



class VehiculosExcel(BaseModel):
    
    Ppu: Optional[str]
    Razon_social: Optional[str]
    Tipo: Optional[str]
    Operaciones : Optional[str]
    Centro_operaciones : Optional[str]
    # Operaciones : Optional[List[str]]
    # Centro_operaciones : Optional[List[str]]
    # Caracteristica: Optional[int]
    # Marca: Optional[int]
    # Modelo: Optional[str]
    # Ano: Optional[int]
    # Rut_colaborador: Optional[str]
    Region: Optional[str]
    # Comuna: Optional[str]
    Gps : Optional[bool]
    Disponible: Optional[bool]
    # Activation_date: Optional[str]
    # Capacidad_carga_kg: Optional[int]
    # Capacidad_carga_m3: Optional[int]
    # Platform_load_capacity_kg: Optional[int]
    # Crane_load_capacity_kg: Optional[int]
    # Permiso_circulacion_fec_venc: Optional[str]
    # Soap_fec_venc: Optional[str]
    # Revision_tecnica_fec_venc: Optional[str]
    # Agency_id: Optional[int]
    # Registration_certificate: Optional[str]
    # Pdf_revision_tecnica: Optional[str]
    # Pdf_soap: Optional[str]
    # Pdf_padron: Optional[str]
    # Pdf_gases_certification: Optional[str]
    # Id_user: Optional[int]
    # Ids_user: Optional[str]
    # Hab_vehiculo : Optional[bool]
    Habilitado : Optional[bool]
    Created_at: Optional[str]

    # Id_gps : Optional[Union[int, str]]
    # Gps : Optional[bool]
    # Imei :Optional[str]
    # Oc_instalacion : Optional[str]
    # Fecha_instalacion : Optional[str]


class VehiculosExcelResumen(BaseModel):
    
    Ppu: Optional[str]
    Razon_social: Optional[str]
    Tipo: Optional[str]
    # Operaciones : Optional[List[str]]
    # Centro_operaciones : Optional[List[str]]
    # Caracteristica: Optional[int]
    # Marca: Optional[int]
    # Modelo: Optional[str]
    # Ano: Optional[int]
    # Rut_colaborador: Optional[str]
    Region: Optional[str]
    # Comuna: Optional[str]
    Gps : Optional[bool]
    Disponible: Optional[bool]
    # Activation_date: Optional[str]
    # Capacidad_carga_kg: Optional[int]
    # Capacidad_carga_m3: Optional[int]
    # Platform_load_capacity_kg: Optional[int]
    # Crane_load_capacity_kg: Optional[int]
    # Permiso_circulacion_fec_venc: Optional[str]
    # Soap_fec_venc: Optional[str]
    # Revision_tecnica_fec_venc: Optional[str]
    # Agency_id: Optional[int]
    # Registration_certificate: Optional[str]
    # Pdf_revision_tecnica: Optional[str]
    # Pdf_soap: Optional[str]
    # Pdf_padron: Optional[str]
    # Pdf_gases_certification: Optional[str]
    # Id_user: Optional[int]
    # Ids_user: Optional[str]
    # Hab_vehiculo : Optional[bool]
    Habilitado : Optional[bool]
    Created_at: Optional[str]

    # Id_gps : Optional[Union[int, str]]
    # Gps : Optional[bool]
    # Imei :Optional[str]
    # Oc_instalacion : Optional[str]
    # Fecha_instalacion : Optional[str]