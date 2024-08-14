from typing import Optional, List
from pydantic import BaseModel

class Dato(BaseModel):
    estado: Optional[int]
    fecha: Optional[str]
    nombre_ruta: Optional[str]
    tipo_ruta: Optional[str]
    id_ruta: Optional[int]
    p_avance: Optional[float]
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
    fm_p_colectas_a_tiempo: Optional[float]
    fm_p_no_colectadas: Optional[float]
    lm_tiempo_ruta: Optional[str]
    lm_estado: Optional[str]
    ppu: Optional[str]
    id_ppu: Optional[int]
    tipo_vehiculo: Optional[str]
    razon_id: Optional[int]
    valor_ruta: Optional[float]
    ruta_cerrada: Optional[bool]
    estado_correcto: Optional[bool]
    patente_igual: Optional[bool]
    driver_ok: Optional[bool]


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