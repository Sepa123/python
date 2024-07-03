from pydantic import BaseModel

class pv(BaseModel):
    sku: str
    # descripcion: str
    # alto: int | float
    # ancho: int | float
    # profundidad: int | float
    # peso_kg : int | float
    # bultos : int
    # id_user : int
    # ids_user: str

class agregarPatente(BaseModel):
    id_user : int
    ids_user: str
    fecha: str
    ruta_meli: str
    id_ppu : int
    id_operacion : int
    id_centro_op : int
    estado : int
    
class updateApp(BaseModel):
    id: int
    estado: bool