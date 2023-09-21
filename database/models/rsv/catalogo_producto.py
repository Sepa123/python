from pydantic import BaseModel


class CatalogoProducto(BaseModel):
    # Id: int
    # Created_at: str
    Codigo_final: str
    Codigo: str
    Producto: str
    Unid_x_paquete: int
    Peso: float
    Ancho: float
    Alto: float
    Largo: float
    Id_user: int
    Ids_user: str
    Color: int
    Habilitado: bool