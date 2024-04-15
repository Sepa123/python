from pydantic import BaseModel
from typing import Optional

class NSEasy(BaseModel):
    Cliente: Optional [str]
    Guia: Optional [str]
    Ruta_hela: Optional [str]
    Direccion : Optional [str]
    Ciudad: Optional[str]
    Region: Optional[str]