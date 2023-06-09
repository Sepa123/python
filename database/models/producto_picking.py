from pydantic import BaseModel
from typing import Optional

class producto_picking(BaseModel):
    Codigo_cliente: str
    Nombre: str
    Calle: str
    Ciudad: str
    Estado: str
    Altitud: str
    longitud: str
    Telefono: str
    Email: str
    Fecha_pedido: str
    Operacion_e_r: str
    Codigo_producto: str
    Descripcion_producto: str
    Cantidad_producto: int
    Peso: int
    Volumen: int
    Dinero: int
    Duracion_min: int
    Ventana_horario_1: str
    Ventana_horario_2: str
    Notas: str
    Agrupador: str
    Email_remitente: str
    Email_pedido: str
    Vehiculo: str
    Habilidades: str