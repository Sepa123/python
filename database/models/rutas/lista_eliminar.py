from pydantic import BaseModel
from typing import Optional

class ListaEliminar(BaseModel):
    id_usuario: Optional[int]
    cliente: Optional[str]
    n_guia: Optional[str]
    cod_pedido: Optional[str]
    cod_producto : Optional[str]
    ids_usuario : Optional[str]
    latitud:  Optional[str]
    longitud:  Optional[str]
    observacion : Optional[str]
    lista : Optional[str]
    nombre_ruta : Optional[str]