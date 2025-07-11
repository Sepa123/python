from pydantic import BaseModel, validator
from decimal import Decimal

class ArmarRutaBloque(BaseModel):
    Codigos : str
    Fecha_ruta : str
    Id_user : int



class UpdateValorRuta(BaseModel):
    guia : str
    ruta : str
    valor_ruta : Decimal


    @validator('valor_ruta')  # Aplicar la misma validación a ambos campos
    def validate_numeric_fields(cls, v):
        if v is not None:
            if v < Decimal('0.00') or v > Decimal('999.99'):
                raise ValueError('El valor debe estar entre 0.00 y 999.99')
            if v.as_tuple().exponent < -2:  # Asegura que solo haya dos decimales
                raise ValueError('El valor puede tener un máximo de 2 decimales')
        return v