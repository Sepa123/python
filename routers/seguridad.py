from fastapi import APIRouter, status,HTTPException
import re
##Modelos 
from database.schema.seguridad.hoja_vida import hoja_vida_producto_schema, datos_usuarios_schema
##Conexiones

from database.client import reportesConnection, UserConnection
from database.hela_prod import HelaConnection


router = APIRouter(tags=["Seguridad"], prefix="/api/seguridad")

conn = reportesConnection()
connUser = UserConnection()
connhela = HelaConnection()

# connHela = HelaConnection()

@router.get("/timeline", status_code=status.HTTP_201_CREATED)
async def registrar_usuario(cod_producto : str):
    result = conn.hoja_vida_producto(cod_producto)
    pattern_portal = r'\bportal-\b'
    pattern_hela = r'\bhela-\b'

    hoja_vida = hoja_vida_producto_schema(result)

    lista_usu_portal = []
    lista_usu_hela = []

    for usu in hoja_vida:
          if re.search(pattern_portal,usu['Created_by']) :
               id = usu['Created_by'].replace("portal-","")
               lista_usu_portal.append(id)
          elif re.search(pattern_hela,usu['Created_by']):
               id = usu['Created_by'].replace("hela-","")
               lista_usu_hela.append(id)
  
    if lista_usu_portal != []:
         usuarios_portal = datos_usuarios_schema(connUser.get_nombre_lista_usuarios(', '.join(lista_usu_portal)))
         
    if lista_usu_hela != []:
         usuarios_hela = datos_usuarios_schema(connhela.get_nombres_lista_usuarios_hela(', '.join(lista_usu_hela)))

    for usu in hoja_vida:
          if re.search(pattern_portal,usu['Created_by']) :
               id = usu['Created_by'].replace("portal-","")
               usu['Created_by'] = next(filter(lambda x: x["Id"] == int(id), usuarios_portal), None)["Nombre"]
          elif re.search(pattern_hela,usu['Created_by']):
               id = usu['Created_by'].replace("hela-","")
               usu['Created_by'] =  next(filter(lambda x: x["Id"] == int(id), usuarios_hela), None)["Nombre"]


    return hoja_vida
    