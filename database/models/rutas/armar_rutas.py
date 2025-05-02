from pydantic import BaseModel

class ArmarRutaBloque(BaseModel):
    Codigos : str
    Fecha_ruta : str
    Id_user : int



class UpdateValorRuta(BaseModel):
    guia : str
    ruta : str
    valor_ruta : int