from fastapi import APIRouter, status,HTTPException

##Modelos 
from database.schema.seguridad.hoja_vida import hoja_vida_producto_schema
##Conexiones

from database.client import reportesConnection
from database.hela_prod import HelaConnection


router = APIRouter(tags=["Seguridad"], prefix="/api/seguridad")

conn = reportesConnection()

# connHela = HelaConnection()

@router.get("/timeline", status_code=status.HTTP_201_CREATED)
async def registrar_usuario(cod_producto : str):
    result = conn.hoja_vida_producto(cod_producto)
    return hoja_vida_producto_schema(result)
    

# @router.post("/password")
# async def cambiar_password(body : loginSchema):
    
#     row = connHela.cambiar_password(hash_password(body.password),body.mail)

#     if row == 1:
#         return {
#             "message": "Se ha actualizado la contraseña correctamente"
#         }
#     else:
#         return {
#             "message": "No se ha actualizado la contraseña"
#         }