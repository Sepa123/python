from pydantic import BaseModel
from typing import Optional

class Usuario(BaseModel):
    Nombre: str
    Mail: str
    Password: str
    Activate: bool
    Rol: str


class CambiarPassword(BaseModel):
    Password_antigua : Optional[str]
    Password_nueva : Optional[str]
    Password_repetida: Optional[str]
    Mail: Optional[str]


class DatosUsuario(BaseModel):
    Id_user: Optional[int]
    Server: Optional[str]
    Telefono: Optional[str]
    Direccion: Optional[str]
    Fecha_nacimiento: Optional[str]
