from pydantic import BaseModel

class reportesEasyRegion(BaseModel):
    origen: str
    RMetropolitana : int
    V_region: int