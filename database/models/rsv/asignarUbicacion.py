from pydantic import BaseModel



class RsVUbicacion (BaseModel):

    bar_code: str
    Ubicacion: str
    verificado: bool
