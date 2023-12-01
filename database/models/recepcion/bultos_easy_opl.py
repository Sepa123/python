from pydantic import BaseModel
from typing import Optional

class BodyBultosOpl(BaseModel):
    Id_ruta: str
    Suborden: str
    Bultos: int
    Tienda: Optional[str]
