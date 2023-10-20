from fastapi import APIRouter, status,HTTPException,Header,Depends
from typing import List
import re
from decouple import config
import lib.beetrack_data as data_beetrack

##Conexiones
from database.client import reportesConnection , UserConnection
from database.hela_prod import HelaConnection
from datetime import datetime

## Modelos

from database.models.beetrack.dispatch_guide import DistpatchGuide
from database.models.beetrack.dispatch import Dispatch , DispatchInsert
from database.models.beetrack.route import Route


router = APIRouter(tags=["Beetrack"], prefix="/api/beetrack")

conn = reportesConnection()
# connHela = HelaConnection()
# connUser = UserConnection()

# Función de dependencia para validar los encabezados
def validar_encabezados(content_type: str = Header(None), x_auth_token: str = Header(None)):
    # Verificar los valores de los encabezados
    if content_type != "application/json":
        raise HTTPException(status_code=400, detail="Content-Type debe ser application/json")
    if x_auth_token != config("SECRET_KEY"):
        print("error con token")
        raise HTTPException(status_code=401, detail="X-AUTH-TOKEN inválido")
    return content_type, x_auth_token



@router.post("/dispatch_guide")
async def post_dispatch_guide(body: DistpatchGuide, headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers
    print("/beetrack/dispatch_guide")
    
    return {
            "body" : body
            }

@router.post("/dispatch")
async def post_dispatch(body : Dispatch, headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers
    # Lista de nombres que deseas buscar
    data = body.dict()

    print("Evento : ", data["event"])
    
    if data["resource"] == 'route' and data["event"] == 'create':
        print("total datos de create",data)
        datos_insert_ruta = data_beetrack.generar_data_insert_creacion_ruta(data)
        conn.insert_beetrack_creacion_ruta(datos_insert_ruta)

    if data["resource"] == 'route' and data["event"] != 'create':
        datos_insert_ruta = data_beetrack.generar_data_insert_creacion_ruta(data)
        print("Datos para actualizar ruta",datos_insert_ruta)
        row = conn.update_route_beetrack_event(datos_insert_ruta)
        print("Tablas actualizadas ", row)
        return {
            "message" : "data recibida correctamente"
            }
    
    if data["resource"] == 'dispatch_guide' and data["event"] == 'create':
        print("Paso por d guide")
        datos_tags_i = data_beetrack.obtener_datos_tags(data["tags"])
        datos_groups_i = data_beetrack.obtener_datos_groups(data["groups"])
        datos_insert_ruta_ty = data_beetrack.generar_data_insert_ruta_transyanez(data,datos_tags_i,datos_groups_i,data["dispatch_guide"])
        
        conn.insert_beetrack_data_ruta_transyanez(datos_insert_ruta_ty)
        return {
            "message" : "data recibida correctamente"
            }
    
    if data["event"] == 'on_route_from_mobile':
        print("lleeegoo")
        print(data)
        
    if data["resource"] != 'dispatch':
        print("es otra cosa xd")
    else:
        print("total datos de update",data)
        datos_tags = data_beetrack.obtener_datos_tags(data["tags"])
        datos_groups = data_beetrack.obtener_datos_groups(data["groups"])
        ## insertar en ruta transyanez
        dato_ruta_ty = data_beetrack.generar_data_insert_ruta_transyanez(data,datos_tags,datos_groups)
        rows = conn.update_ruta_ty_event(dato_ruta_ty)

        print("tablas actualizadas de ruta_ty ",rows)

        for item in data["items"]:
            waypoint = data["waypoint"]
            if waypoint is None:
                waypoint = {}
                waypoint["latitude"] = ""
                waypoint["longitude"] = ""
                
            print(waypoint)
            ## insertar en ruta transyanez por item
            dato_insert = data_beetrack.generar_data_insert(data,item,datos_tags,waypoint)
            print("dato_insertar a dispatch",dato_insert)
            conn.insert_beetrack_dispatch_guide_update(dato_insert)
            
            
        

    print("/beetrack/dispatch")
    return {
            "message" : "data recibida correctamente"
            }

@router.post("/route")
async def post_route(body : Route , headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers
    print("/beetrack/route")
    print(body)
    return {
            "body" : body
            }