from typing import Optional, List
from decimal import Decimal
from pydantic import BaseModel, ValidationError, validator

class Dato(BaseModel):
    estado: Optional[int]
    fecha: Optional[str]
    nombre_ruta: Optional[str]
    tipo_ruta: Optional[int]
    id_ruta: Optional[int]
    p_avance: Optional[Decimal]
    avance: Optional[int]
    fm_total_paradas: Optional[int]
    fm_paqueteria_colectada: Optional[int]
    fm_estimados: Optional[int]
    fm_preparados: Optional[int]
    lm_fallido: Optional[int]
    lm_pendiente: Optional[int]
    lm_spr: Optional[int]
    lm_entregas: Optional[int]
    driver: Optional[str]
    fm_p_colectas_a_tiempo: Optional[Decimal]
    fm_p_no_colectadas: Optional[Decimal]
    lm_tiempo_ruta: Optional[str]
    lm_estado: Optional[str]
    ppu: Optional[str]
    id_ppu: Optional[int]
    tipo_vehiculo: Optional[str]
    razon_id: Optional[int]
    valor_ruta: Optional[Decimal]
    ruta_cerrada: Optional[bool]
    estado_correcto: Optional[bool]
    patente_igual: Optional[bool]
    driver_ok: Optional[bool]
    ruta_meli : Optional[str]
    kilometro: Optional[Decimal]
    observacion: Optional[str]

    @validator('p_avance', 'fm_p_colectas_a_tiempo','fm_p_no_colectadas','valor_ruta')  # Aplicar la misma validación a ambos campos
    def validate_numeric_fields(cls, v):
        if v is not None:
            if v < Decimal('0.00') or v > Decimal('999.99'):
                raise ValueError('El valor debe estar entre 0.00 y 999.99')
            if v.as_tuple().exponent < -2:  # Asegura que solo haya dos decimales
                raise ValueError('El valor puede tener un máximo de 2 decimales')
        return v
    
    @validator('kilometro')
    def validate_numeric_fields_kilometro(cls, v):
        if v is not None:
            if v < Decimal('0.0') or v > Decimal('999999999.9'):
                raise ValueError('El valor debe estar entre 0.0 y 999999999.9')
            if v.as_tuple().exponent < -1:  # Asegura que solo haya un decimal
                raise ValueError('El valor puede tener un máximo de 1 decimal')
        return v


class DataSupervisor(BaseModel):
    id: Optional[int]
    created_at: Optional[str]
    id_usuario: Optional[int]
    ids_usuario: Optional[str]
    latitud: Optional[str]
    longitud: Optional[str]
    operacion: Optional[str]
    id_operacion: Optional[int]
    id_centro_operacion: Optional[int]
    datos: Optional[List[Dato]]

