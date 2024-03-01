from pydantic import BaseModel
from typing import Optional, Union

class DifFechaEasy(BaseModel):
    Cliente: Optional[str]
    Ingreso_sistema: Optional[str]
    Fecha_compromiso: Optional[str]
    Ultima_actualizacion: Optional[str]
    Dias_ejecucion: Optional[int]
    Cod_pedido: Optional[int]
    Id_entrega: Optional[str]
    Direccion: Optional[str]
    Comuna: Optional[str]
    Descripcion: Optional[str]
    Unidades: Union[int,str,None]
    Estado: Optional[str]
    Subestado: Optional[str]
