from datetime import datetime
from pydantic import BaseModel



class bodyPaqueteYBitacora(BaseModel):
    id_usuario: int
    ids_usuario : str
    sucursal: int
    id_etiqueta: int
    bar_code: str
    momento : int
    lat: str
    lng:  str