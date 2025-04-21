from typing import Optional , List
from pydantic import BaseModel



class Cuota(BaseModel):
    Id_user: Optional[int]
    Ids_user: Optional[str]
    Id_origen_desc: Optional[int]
    Ids_origen_desc: Optional[str]
    Fecha_comp: Optional[str]
    Valor_cuota: Optional[int]
    Numero_cuota: Optional[str]
    Origen: Optional[str]
    Cobrada: Optional[bool]
    Oc_cobro: Optional[str]


    # id_user, ids_user,  id_origen_descuento, ids_origen_descuento, fecha_cobro, valor_cuota, numero_cuota, origen, cobrada, oc_cobro

class DescuentoManual (BaseModel):
    Id_user: Optional[int]
    Ids_user: Optional[str]
    Fecha_evento: Optional[str]
    Pantente: Optional[int]
    Razon_social: Optional[int]
    Ruta: Optional[str]
    Etiqueta: Optional[int]
    Descripcion: Optional[str]
    Adjunto: Optional[str]
    Cant_cuotas : Optional[int]
    Monto: Optional[int]
    Cuotas : List[Cuota] 
    Id_operacion : Optional[int]
    Id_cop: Optional[int]



class ActualizarDescuento(BaseModel):
    Id_detalle: Optional[int]
    Cobrado: Optional[bool]
    Oc_cobro: Optional[str]



# Modelo para la solicitud
class UpdateColumnRequest(BaseModel):
    id: int  # ID del registro a actualizar
    nuevo_valor: bool  # Nuevo valor para la columna
