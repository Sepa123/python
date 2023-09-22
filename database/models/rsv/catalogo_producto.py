from pydantic import BaseModel
from typing import Optional

class CatalogoProducto(BaseModel):
    # Id: int
    # Created_at: str
    Codigo_final: str
    Codigo: str
    Producto: str
    Unid_x_paquete: int
    Peso: Optional[float]
    Ancho: Optional[float]
    Alto: Optional[float]
    Largo: Optional[float]
    Id_user: int
    Ids_user: str
    Color: int
    Habilitado: Optional[bool]
    Precio_unitario : Optional[int]
    Ubicacion : Optional[str]
    Codigo_Original : Optional[str]
    