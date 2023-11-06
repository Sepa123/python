from pydantic import BaseModel

class UnidadEtiqueta(BaseModel):
    codigo : str
    unid_con_etiqueta: bool