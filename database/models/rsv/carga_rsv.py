from pydantic import BaseModel


class CargaRSV (BaseModel):
    id: int
    created_at: str
    fecha_ingreso: str
    id_usuario: int
    ids_usuario: str
    nombre_carga: str
    codigo: str
    color: int
    paquetes: int
    unidades: int
    verificado: bool