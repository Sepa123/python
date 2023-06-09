from pydantic import BaseModel
from typing import Optional

class RutaManual(BaseModel):
    Id_ruta: Optional[int]
    Nombre_ruta: Optional[str]
    Codigo_cliente: str
    Nombre: str
    Calle: str
    Ciudad: Optional[str]
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
    Ventana_horaria_2: Optional[str]
    Notas: Optional[str]
    Agrupador: Optional[str]
    Email_remitentes: Optional[str]
    Eliminar_pedido: Optional[str]
    Vehiculo: Optional[str]
    Habilidades: Optional[str]
    SKU: Optional[int]
    Pistoleado: Optional[str]
    Tamaño: Optional[str]
    Estado: Optional[str]
    En_ruta: Optional[str]
    Created_by: Optional[int]
    Fecha_ruta : Optional[str]