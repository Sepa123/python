from pydantic import BaseModel
from typing import Union

class pv(BaseModel):
    sku: str
    descripcion: str
    alto: Union[int, float, None]
    ancho: Union[int, float, None]
    profundidad: Union[int, float, None]
    peso_kg : Union[int, float, None]
    bultos : int
    id_user : int
    ids_user: str

class agregarPatente(BaseModel):
    id_user : int
    ids_user: str
    fecha: str
    ruta_meli: str
    id_ppu : int
    id_operacion : int
    id_centro_op : int
    estado : int
    
class updateApp(BaseModel):
    id: int
    estado: bool