from pydantic import BaseModel

class UpdateVerified(BaseModel):
    cod_producto: str
    id_usuario : str