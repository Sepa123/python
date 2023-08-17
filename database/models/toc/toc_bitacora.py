from pydantic import BaseModel
from typing import Optional


class BitacoraToc(BaseModel):
    Codigo_producto: Optional[str]
    Fecha: Optional[str]
    PPU: Optional[str]
    Guia: Optional[str]
    Cliente: Optional[str]
    Region: Optional[str]
    Comuna: Optional[str]
    Estado: Optional[str]
    Subestado: Optional[str]
    Driver: Optional[str]
    Nombre_cliente: Optional[str]
    Fecha_compromiso: Optional[str]
    Direccion_correcta: Optional[str]
    Comuna_correcta: Optional[str]
    Fecha_reprogramada: Optional[str]
    Observacion: Optional[str]
    Subestado_esperado: Optional[str]
    Id_transyanez: Optional[int]
    Ids_transyanez: Optional[str]
    Id_usuario: Optional[int]
    Ids_usuario: Optional[str]
    Alerta: Optional[bool]
    Codigo1Str: Optional[str]
    Codigo1: Optional[int]