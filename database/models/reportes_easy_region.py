from pydantic import BaseModel

class reportesEasyRegion(BaseModel):
    Origen: str
    R_metropolitana : int
    V_region: int