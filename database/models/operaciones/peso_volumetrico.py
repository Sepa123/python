from pydantic import BaseModel
from typing import Union

class PesoVolumetrico(BaseModel):
    sku: str
    descripcion: str
    alto: Union[int, float, None]
    ancho: Union[int, float, None]
    profundidad: Union[int, float, None]
    peso_kg : Union[int, float, None]
    bultos : int
    id_user : int
    ids_user: str