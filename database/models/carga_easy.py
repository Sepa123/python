from pydantic import BaseModel
from typing import Optional


class CargaEasy(BaseModel):
    total: int
    verificado: int

