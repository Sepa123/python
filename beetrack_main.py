from fastapi import  status,HTTPException,Header,Depends,FastAPI 
from typing import List , Dict ,Union
import re
from decouple import config
import lib.beetrack_data as data_beetrack
import httpx
import lib.guardar_datos_json as guardar_json
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from datetime import datetime, timedelta

##Conexiones
from database.client import reportesConnection , UserConnection
from database.hela_prod import HelaConnection
from datetime import datetime

## Modelos

from database.models.beetrack.dispatch_guide import DistpatchGuide
from database.models.beetrack.dispatch import Dispatch , DispatchInsert
from database.models.beetrack.route import Route


# app = APIRouter(tags=["Beetrack"], prefix="/api/beetrack")

app = FastAPI(docs_url="/api/beetrack/docs", redoc_url="/api/beetrack/redoc")

conn = reportesConnection()


origins = [
    "http://localhost:4200",
    "http://localhost:8080",
    "http://18.220.116.139:80",
    "http://18.220.116.139:88",
    "http://34.225.63.221:84",
    "http://34.225.63.221:84/#/login",
    "https://hela.transyanez.cl",
    "http://34.225.63.221",
    "https://testhl1.azurewebsites.net",
    "http://15.229.226.244",
    "http://prueba.transyanez.cl",
    "https://prueba.transyanez.cl",
    "https://garm.transyanez.cl",
    "http://garm.transyanez.cl"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=10000)

# Función de dependencia para validar los encabezados
def validar_encabezados(content_type: str = Header(None), x_auth_token: str = Header(None)):
    # Verificar los valores de los encabezados
    if content_type != "application/json":
        raise HTTPException(status_code=400, detail="Content-Type debe ser application/json")
    if x_auth_token != config("SECRET_KEY"):
        print("error con token")
        raise HTTPException(status_code=401, detail="X-AUTH-TOKEN inválido")
    return content_type, x_auth_token



@app.post("/api/v2/beetrack/dispatch_guide")
async def post_dispatch_guide(body: DistpatchGuide, headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers

    return {
            "body" : body
            }

@app.post("/api/v2/beetrack/dispatch")
async def post_dispatch(body : Dispatch, headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers
    # Lista de nombres que deseas buscar
    data = body.dict()

    # print("Evento : ", data["event"])
    
    if data["resource"] == 'route' and data["event"] == 'create':
        # print("total datos de create",data)
        datos_insert_ruta = data_beetrack.generar_data_insert_creacion_ruta(data)
        conn.insert_beetrack_creacion_ruta(datos_insert_ruta)

    if data["resource"] == 'route' and data["event"] in ['start', 'finish']:
        datos_insert_ruta = data_beetrack.generar_data_insert_creacion_ruta(data)
        # print("Datos para actualizar ruta",datos_insert_ruta)
        row = conn.update_route_beetrack_event(datos_insert_ruta)
        # print("Tablas actualizadas ", row)
        return {
            "message" : "data recibida correctamente"
            }
    
    if data["resource"] == 'dispatch' and data["event"] == 'update':
        datos_create = {
                        "ruta_id" : data["route_id"],
                        "guia" : data["guide"]
                        }

        resultado = conn.verificar_si_ruta_existe(datos_create)
        # print("resultado :",resultado)
        if len(resultado) == 0:
            # print("Paso por d guide")
            # print("d guide : ",data)
            datos_tags_i = data_beetrack.obtener_datos_tags(data["tags"])
            datos_groups_i = data_beetrack.obtener_datos_groups(data["groups"])
            datos_insert_ruta_ty = data_beetrack.generar_data_update_ruta_transyanez(data,datos_tags_i,datos_groups_i)
            conn.insert_beetrack_data_ruta_transyanez(datos_insert_ruta_ty)

            if datos_groups_i["Cliente"] == "Electrolux":
                patron = r'\D+'
                factura = re.sub(patron, '', datos_tags_i["FACTURA"])
                ahora = datetime.now()

                datos = { 
                    "Numero" : factura,
                    "Hora_registro": str(ahora)
                }

                guardar_json.guardar_datos_a_archivo_existente_cf(datos,ahora,'info_factura')

            return {
                "message" : "data recibida correctamente"
                }
        else :
            # print("total datos de update",data)
            datos_tags = data_beetrack.obtener_datos_tags(data["tags"])
            datos_groups = data_beetrack.obtener_datos_groups(data["groups"])
            ## insertar en ruta transyanez
            dato_ruta_ty = data_beetrack.generar_data_update_ruta_transyanez(data,datos_tags,datos_groups)
            rows = conn.update_ruta_ty_event(dato_ruta_ty)

            if datos_groups["Cliente"] == "Electrolux":
                patron = r'\D+'
                factura = re.sub(patron, '', datos_tags["FACTURA"])

                ahora = datetime.now()

                datos = { 
                    "Numero" : factura,
                    "Hora_registro": str(ahora)
                }

                guardar_json.guardar_datos_a_archivo_existente_cf(datos,ahora,'info_factura')
                
    return {
            "message" : "data recibida correctamente"
            }

@app.post("/api/v2/beetrack/route")
async def post_route(body : Route , headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers

    # print(body)
    return {
            "body" : body
            }


@app.post("/api/v2/beetrack/Enviar/loquesea")
async def post_route(body : Union[Dict, List[Dict]] ):
    # print(body)
    return {
            "body" : body
            }