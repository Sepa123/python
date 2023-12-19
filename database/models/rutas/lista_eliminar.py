from pydantic import BaseModel
from typing import Optional

class ListaEliminar(BaseModel):
    lista : Optional[str]
    nombre_ruta : str