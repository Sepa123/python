from pydantic import BaseModel
from datetime import time
from typing import Optional

class RutaBeetrackHoy (BaseModel):
    Numero: int
    Ruta: str
    Patente: str
    Driver: str
    Inicio: str
    Region: str
    Total_pedidos: int
    Once: str
    Una: str
    Tres: str
    Cinco: str
    Seis: str
    Ocho: str
    Entregados: str
    No_entregados: str
    Porcentaje: int