from pydantic import BaseModel

class ArmarRutaBloque(BaseModel):
    Codigos : str
    Fecha_ruta : str
    Id_user : int