from pydantic import BaseModel
from typing import List , Optional

class ListaFuncion(BaseModel):
    Esquema : Optional[str]
    Nombre_funcion : Optional[str]
    Tipo_funcion : Optional[int]
    Descripcion : Optional[str]
    arrParametros : Optional[List[str]]
    arrComentario : Optional[List[str]]
    arrPalabras_clave : Optional[List[str]]
    arrTablas_impactadas : Optional[List[str]]
