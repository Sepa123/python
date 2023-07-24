from pydantic import BaseModel

class UpdateVerified(BaseModel):
    cod_pedido: str
    id_usuario : str