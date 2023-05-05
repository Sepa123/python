from pydantic import BaseModel 
from typing import Optional



class resumen_vehiculos(BaseModel):
    compañia : str
    region_origen : str
    patente: str
    estado: str
    tipo: str
    caracteristicas : str
    marca: str
    modelo : str
    año: int
    region: str
    comuna:str


