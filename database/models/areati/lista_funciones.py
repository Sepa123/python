from pydantic import BaseModel
from typing import List

class ListaFuncion(BaseModel):
    Id :int
    Fecha_creacion : str
    Esquema : str
    Tipo_funcion : int
    Descripcion : str
    Parametros : List[str]
    Comentario_parametros : List[str]
    Palabras_clave : List[str]
    Tablas_impactadas : List[str]
