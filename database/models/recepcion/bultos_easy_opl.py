from pydantic import BaseModel
from typing import Optional

class BodyBultosOpl(BaseModel):
    Id_ruta: str
    Suborden: str
    Bultos: int
    Tienda: Optional[str]
    latitud : Optional [str]
    longitud : Optional[str]
    id_usuario: Optional[int]
    ids_usuario: Optional[str]
    cliente : Optional[str]
    n_guia : Optional[str]
    observacion : Optional[str]
