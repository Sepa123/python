from typing import Optional , List
from datetime import datetime
from pydantic import BaseModel

class CargaQuadmind(BaseModel):
    Codigo_cliente: str
    Nombre: str
    Calle: str
    Ciudad: str
    Provincia: str
    Latitud: Optional[str]
    Longitud: Optional[str]
    Telefono: str
    Email: str
    Codigo_pedido: str
    Fecha_pedido: str
    Operacion: str
    Codigo_producto: str
    Descripcion_producto: str
    Cantidad_producto: int
    Peso: int
    Volumen: int
    Dinero: int
    Duracion_min: int
    Ventana_horaria_1: str
    Ventana_horaria_2: str
    Notas: str
    Agrupador: str
    Email_remitentes: str
    Eliminar_pedido: str
    Vehiculo: str
    Habilidades: str
    arrayCodigo: Optional[list]
    arrayDescripcion: Optional[list]