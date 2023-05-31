from pydantic import BaseModel
from typing import Optional

class reportesEasyRegion(BaseModel):
    Origen: str
    R_metropolitana : int
    V_region: int