from typing import Optional , List
from datetime import datetime
from pydantic import BaseModel

class CargaQuadmind(BaseModel):
    codigo_cliente: str
    nombre: str
    calle: str
    ciudad: str
    provincia: Optional[str]
    latitud: Optional[str]
    longitud: Optional[str]
    telefono: str
    email: str
    codigo_pedido: str
    fecha_pedido: datetime
    operacion: str
    codigo_producto: str
    descripcion_producto: str
    cantidad_producto: int
    peso: int
    volumen: int
    dinero: int
    duracion_min: int
    ventana_horaria_1: str
    ventana_horaria_2: str
    notas: str
    agrupador: str
    email_remitentes: str
    eliminar_pedido: str
    vehiculo: str
    habilidades: str
    # arrayCodigo: Optional[List(str)]
    # arrayDescripcion: Optional[List(str)] 