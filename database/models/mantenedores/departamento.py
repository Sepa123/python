from pydantic import BaseModel
from typing import Optional


class DepartamentoInventario(BaseModel):

    id_user: Optional [str]
    ids_user: Optional [str]
    nombre: Optional [str]