from pydantic import BaseModel

class CargaEasy(BaseModel):
    total: int
    verificado: int

