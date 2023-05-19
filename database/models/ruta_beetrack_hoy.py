from pydantic import BaseModel

class ruta_beetrack_hoy (BaseModel):
    numero: int
    ruta: str
    patente: str
    driver: str
    inicio: str
    region: str
    total_pedidos: int
    once: str
    una: str
    tres: str
    cinco: str
    seis: str
    ocho: str
    entregados: str
    no_entregados: str
    porcentaje: int