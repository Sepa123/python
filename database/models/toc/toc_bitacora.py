from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class BitacoraToc(BaseModel):
    Codigo_producto: Optional[str]
    Fecha: Optional[datetime]
    PPU: Optional[str]
    Guia: Optional[str]
    Cliente: Optional[str]
    Region: Optional[str]
    Comuna: Optional[str]
    Estado: Optional[str]
    Subestado: Optional[str]
    Driver: Optional[str]
    Nombre_cliente: Optional[str]
    Fecha_compromiso: Optional[datetime]
    Direccion_correcta: Optional[str]
    Comuna_correcta: Optional[str]
    Fecha_reprogramada: Optional[datetime]
    Observacion: Optional[str]
    Subestado_esperado: Optional[str]
    Id_transyanez: Optional[str]
    Ids_transyanez: Optional[str]
    Id_usuario: Optional[int]
    Ids_usuario: Optional[str]
    Alerta: Optional[bool]