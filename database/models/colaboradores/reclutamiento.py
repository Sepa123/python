from pydantic import BaseModel, EmailStr,  validator
from typing import Optional
from decimal import Decimal

class Reclutamiento(BaseModel):
    Id_reclutamiento : Optional[int]
    Id_user: int
    Ids_user: Optional[str]
    Modificacion: Optional[str]
    Latitud: Optional[str]
    Longitud: Optional[str]
    Origen: Optional[str]
    Region: Optional[int]
    Operacion_postula : Optional[int]
    Nombre_contacto : str
    Telefono : Optional[str]
    Origen_contacto : Optional[int]
    Estado_contacto : Optional[int]
    Motivo_subestado : Optional[int]
    Contacto_ejecutivo : Optional[int]
    Razon_social : Optional[str]
    Rut_empresa : Optional[str]
    Capacidad : Optional[str]
    Comuna: Optional[int]  # Entero para la comuna
    Cant_vehiculos: Optional[int]  # Número opcional de vehículos, puede ser None
    Ppu: Optional[str] # Placa patente única, máximo 6 caracteres
    Tipo_vehiculo: Optional[int]  # Entero para tipo de vehículo
    Tipo_carroceria: Optional[int]  # Entero para tipo de carrocería
    Tipo_adicionales: Optional[int]  # Entero para tipo de adicionales
    Metros_cubicos: Optional[Decimal]  # Decimal con precisión 5,2
    Inicio_actividades_factura: bool  # Booleano para inicio de actividades de facturación
    Giro: Optional[int]  # Entero para giro comercial
    Correo: Optional[str]


    @validator('Metros_cubicos')  # Aplicar la misma validación a ambos campos
    def validate_numeric_fields(cls, v):
        if v is not None:
            if v < Decimal('0.00') or v > Decimal('999.99'):
                raise ValueError('El valor debe estar entre 0.00 y 999.99')
            if v.as_tuple().exponent < -2:  # Asegura que solo haya dos decimales
                raise ValueError('El valor puede tener un máximo de 2 decimales')
        return v
    # Internalizado : Optional[bool]



class ComentarioRecluta(BaseModel):
    Id : int ## estatus_comentario
    Id_recluta: int ## id_recluta
    Id_user: int
    Ids_user: Optional[str]
    Latitud: Optional[str]
    Longitud: Optional[str]
    Comentario : Optional[str]



