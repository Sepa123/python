from pydantic import BaseModel

class asignarValor(BaseModel):
    id_ruta: int
    valor_ruta: float
    id_user: int
    ids_user: str
