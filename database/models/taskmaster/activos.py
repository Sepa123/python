from typing import Optional
from fastapi import UploadFile
from pydantic import BaseModel

class Activo(BaseModel):
    Id: Optional[int]
    Created_at: Optional[str]  # Using datetime for better date handling
    Last_update: Optional[str]
    Id_user: int
    Id_area: Optional[int]
    Categoria: int
    Nombre_equipo: str
    Marca: Optional[str]
    Modelo: Optional[str]
    Codigo: Optional[str]  # Adding 'Codigo' as it is in the SQL schema
    Descripcion: Optional[str]
    Region: Optional[int]
    Comuna: Optional[int]
    Direccion: Optional[str]
    Latitud: Optional[str]
    Longitud: Optional[str]
    Imagen_1: Optional[str]
    Imagen_2: Optional[str]
    Imagen_3: Optional[str]
    Manual_pdf: Optional[str]
    Fecha_adquisicion: Optional[str]  # Date type to match the SQL 'date' column
    Id_estado: Optional[int]
    Garantia: Optional[str]
    Proveedor: Optional[str]
    Valor_adquisicion: Optional[int]
    Vida_util: Optional[int]
    Id_responsable: Optional[int]
    Fecha_ultimo_mantenimiento: Optional[str]  # Date type for the last maintenance date
    Observaciones: Optional[str]
    Activo: Optional[bool]
    Fecha_baja: Optional[str]  # Optional date for the asset's deactivation


class ImagenesActivo(BaseModel):
    imagen1_png: Optional[UploadFile] = None
    imagen2_png: Optional[UploadFile] = None
    imagen3_png: Optional[UploadFile] = None
    id_activo: Optional[int] = None