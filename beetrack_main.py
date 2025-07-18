import json
import os
import time
from fastapi import  Request, status,HTTPException,Header,Depends,FastAPI
from typing import List , Dict ,Union
import re
from decouple import config
import psycopg2
from pydantic import BaseModel
from database.models.transporte.trabajemos import ContactoExterno
import lib.beetrack_data as data_beetrack
import httpx
import lib.guardar_datos_json as guardar_json
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from datetime import datetime, timedelta
from lib.password import verify_password, hash_password

##Conexiones
from database.client import reportesConnection , UserConnection
from database.hela_prod import HelaConnection
from datetime import datetime

## Modelos
from database.models.token import TokenPayload
from database.models.beetrack.dispatch_guide import DistpatchGuide
from database.models.beetrack.dispatch import Dispatch , DispatchInsert
from database.models.beetrack.route import Route
from database.models.dispatch_paris.distpatch import ActualizacionGuiaStr, CreacionGuia, CreacionRuta, ActualizacionGuia ,Item


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
    "http://garm.transyanez.cl",
    'http://localhost:4001',
    'http://trabajemos.transyanez.cl',
    'https://trabajemos.transyanez.cl'
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
        print("error con token ",x_auth_token)
        # print(x_auth_token)
        raise HTTPException(status_code=401, detail="X-AUTH-TOKEN inválido")
    return content_type, x_auth_token



@app.post("/api/v2/beetrack/dispatch_guide")
async def post_dispatch_guide(body: DistpatchGuide, headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers

    return {
            "body" : body
            }


@app.get("/api/v2/productos/paris")
async def post_dispatch_guide():
    # content_type, x_auth_token = headers

    datos = conn.read_clientes_de_paris()

    # print("datos",datos)

    return datos[0]


@app.post("/api/v2/beetrack/route")
async def post_route(body : Route , headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers

    return {
            "body" : body
            }

### endpoints paris

def crear_vehiculo_paris(patente): ### esto es para crear un vehiculo en paris
    # URL del endpoint
    url = f'https://paris.dispatchtrack.com/api/external/v1/trucks'

    # Encabezados
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Thunder Client (https://www.thunderclient.com)',
        'X-AUTH-TOKEN': config("SECRET_KEY_PARIS"),
    }

    # Cuerpo de la solicitud (puedes modificar esto según lo que necesites enviar)
    payload = {
        "identifier": patente
    }

    # Realizamos la solicitud PUT
    with httpx.Client() as client:
        response = client.post(url, headers=headers, json=payload)

    # Verificamos la respuesta
    if response.status_code == 200:
        print("Solicitud POST exitosa:", response.json())

        # body = response.json()
        # response_data = body.get("response", {})  # Usamos .get() para evitar errores si no existe la clave

        # # Extraemos los datos de interés
        # vehicle_id = response_data.get("id")
        # truck = response_data.get("truck")
        # vehicle_type = response_data.get("vehicle_type")
        # truck_created = response_data.get("truck_created")
        # created = response_data.get("created")

        # # conn.guardar_patente_paris(vehicle_id,truck)

        # timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # filename = f"POST_vehiculo_Paris_{timestamp}.txt"

        #     # body = await request.json()  # Obtener el cuerpo como JSON
        #     # Guardar el contenido del JSON en un archivo de texto
        # with open(filename, "w") as f:
        #     json.dump(body, f, indent=4)
    else:

        # timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # filename = f"rPOST_vehiculo_Paris_error_{timestamp}.txt"

        #     # body = await request.json()  # Obtener el cuerpo como JSON
        #     # Guardar el contenido del JSON en un archivo de texto
        # with open(filename, "w") as f:
        #     f.write(response.text)
        #     f.write("\n")
        #     json.dump(payload, f, indent=4)

        print(f"Error en la solicitud POST: {response.status_code}")
        print(response.text)

def crear_ruta_paris(payload): ### esto es para crear una ruta en paris
    # URL del endpoint
    url = f'https://paris.dispatchtrack.com/api/external/v1/routes'

    # Encabezados
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Thunder Client (https://www.thunderclient.com)',
        'X-AUTH-TOKEN': config("SECRET_KEY_PARIS"),
    }

    # Cuerpo de la solicitud (puedes modificar esto según lo que necesites enviar)
    payload = payload

    # Realizamos la solicitud PUT
    with httpx.Client() as client:
        response = client.post(url, headers=headers, json=payload)

    # Verificamos la respuesta
    if response.status_code == 200:
        print("Solicitud POST exitosa:", response.json())

        body = response.json()
        response_data = body.get("response", {})  # Usamos .get() para evitar errores si no existe la clave

        # Extraemos los datos de interés
        route_id = response_data.get("route_id")


        # timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # filename = f"POST_Ruta_Paris_{timestamp}.txt"

        #     # body = await request.json()  # Obtener el cuerpo como JSON
        #     # Guardar el contenido del JSON en un archivo de texto
        # with open(filename, "w") as f:
        #     json.dump(body, f, indent=4)
        #     f.write("\n")
        #     json.dump(payload, f, indent=4)

        

        return route_id

    else:

        # timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # filename = f"rPOST_vehiculo_Paris_error_{timestamp}.txt"

        #     # body = await request.json()  # Obtener el cuerpo como JSON
        #     # Guardar el contenido del JSON en un archivo de texto
        # with open(filename, "w") as f:
        #     f.write(response.text)
        #     f.write("\n")
        #     json.dump(payload, f, indent=4)

        print(f"Error en la solicitud POST: {response.status_code}")
        print(response.text)

        return None

def send_put_update_ruta(payload, route_id): ### esto es para actualizar una ruta en paris
    # URL del endpoint
    url = f'https://paris.dispatchtrack.com/api/external/v1/routes/{route_id}'
    print(url)


    ## folder

    folder = "resp_api_paris"
    # Encabezados
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Thunder Client (https://www.thunderclient.com)',
        'X-AUTH-TOKEN': config("SECRET_KEY_PARIS"),
    }

    # Cuerpo de la solicitud (puedes modificar esto según lo que necesites enviar)
    payload = payload

    # Realizamos la solicitud PUT
    with httpx.Client() as client:
        response = client.put(url, headers=headers, json=payload)

    # Verificamos la respuesta
    if response.status_code == 200:
        print("Solicitud PUT exitosa:", response.json())
        body = response.json()

        
        # filename = os.path.join(folder, f"datos_ruta_parasabersillegan_ACTUALIZACION{timestamp}.txt")
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename =  os.path.join(folder,f"r_update_route_Paris_route_id_{route_id}_{timestamp}.txt")


            # body = await request.json()  # Obtener el cuerpo como JSON
            # Guardar el contenido del JSON en un archivo de texto
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(body, f, indent=4,ensure_ascii=False)
            f.write("\n")
            json.dump(payload, f, indent=4,ensure_ascii=False)
    else:

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = os.path.join(folder,f"r_update_route_Paris_error_route_id_{route_id}_{timestamp}.txt")

            # body = await request.json()  # Obtener el cuerpo como JSON
            # Guardar el contenido del JSON en un archivo de texto
        with open(filename, "w") as f:
            f.write(response.text)
            f.write("\n")
            json.dump(payload, f, indent=4,ensure_ascii=False)

        print(f"Error en la solicitud PUT: {response.status_code}")
        print(response.text)


def send_put_request(payload, codigo_guia):
    # URL del endpoint
    url = f'https://paris.dispatchtrack.com/api/external/v1/dispatches/{codigo_guia}'

    

    # Encabezados
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Thunder Client (https://www.thunderclient.com)',
        'X-AUTH-TOKEN': config("SECRET_KEY_PARIS"),
    }

    ## folder

    folder = "resp_api_paris"

    # Cuerpo de la solicitud (puedes modificar esto según lo que necesites enviar)
    payload = payload

    # Realizamos la solicitud PUT
    with httpx.Client() as client:
        response = client.put(url, headers=headers, json=payload)

    # Verificamos la respuesta
    if response.status_code == 200:
        print("Solicitud PUT exitosa:", response.json())
        body = response.json()

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = os.path.join(folder,f"r_update_dispatch_Paris_{timestamp}.txt")


            # body = await request.json()  # Obtener el cuerpo como JSON
            # Guardar el contenido del JSON en un archivo de texto
        with open(filename, "w") as f:
            json.dump(body, f, indent=4)
    else:

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = os.path.join(folder,f"r_update_dispatch_Paris_error_codigo_{codigo_guia}_{timestamp}.txt")

            # body = await request.json()  # Obtener el cuerpo como JSON
            # Guardar el contenido del JSON en un archivo de texto
        with open(filename, "w") as f:
            f.write(response.text)

        print(f"Error en la solicitud PUT: {response.status_code}")
        print(response.text)




def send_put_request_paris_yanez(payload, codigo_guia): #### No la utilizo
    # URL del endpoint
    url = f'https://cluster-staging.dispatchtrack.com/api/external/v1/dispatches/{codigo_guia}'

    # Encabezados
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Thunder Client (https://www.thunderclient.com)',
        'X-AUTH-TOKEN': config("SECRET_KEY_PARIS_YANEZ"),
    }

    # Cuerpo de la solicitud (puedes modificar esto según lo que necesites enviar)
    payload = payload

    # Realizamos la solicitud PUT
    with httpx.Client() as client:
        response = client.put(url, headers=headers, json=payload)

    # Verificamos la respuesta
    if response.status_code == 200:
        print("Solicitud PUT exitosa:", response.json())
    else:
        print(f"Error en la solicitud PUT: {response.status_code}")
        print(response.text)


def get_update_estados_paris_yanez( codigo_guia): ### creo que no la utilizo
    # URL del endpoint
    url = f'https://cluster-staging.dispatchtrack.com/api/external/v1/dispatches/{codigo_guia}'

    # Encabezados
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Thunder Client (https://www.thunderclient.com)',
        'X-AUTH-TOKEN': config("SECRET_KEY_PARIS_YANEZ"),
    }

    # Realizamos la solicitud PUT
    with httpx.Client() as client:
        response = client.get(url, headers=headers)

    # Verificamos la respuesta
    if response.status_code == 200:
        datos = response.json()
        status_id = datos.get("response", {}).get("status_id", None)
        substatus_code = datos.get("response", {}).get("substatus_code", None)

        # Devolvemos solo estos dos valores
        return {"status_id": status_id, "substatus_code": substatus_code}
    else:
        print(f"Error en la solicitud PUT: {response.status_code}")
        print(response.text)





def verificar_si_ruta_yanez_existe_despachos(ruta_id): 

    ##### version por bd

    troncales = []

    info_despachos = []


    info_despachos = conn.obtener_guias_troncales_paris(ruta_id)[0]

    print(info_despachos)

    if info_despachos is not None:
        troncales = [False]
    else:
        troncales = [True]

    return  info_despachos, troncales




    #  # URL del endpoint
    # url = f'https://cluster-staging.dispatchtrack.com/api/external/v1/routes/{ruta_id}'

    # # Encabezados
    # headers = {
    #     'Accept': '*/*',
    #     'User-Agent': 'Thunder Client (https://www.thunderclient.com)',
    #     'X-AUTH-TOKEN': config("SECRET_KEY_PARIS_YANEZ"),
    # }

    # # Realizamos la solicitud PUT
    # with httpx.Client() as client:
    #     response = client.get(url, headers=headers)

    
    # troncales = []

    # info_despachos = []

    # # Verificamos la respuesta
    # if response.status_code == 200:
    #     # print("Solicitud GET exitosa:", response.json())

    #     data = response.json()

        
    #     for despachos in data['response']['route']['dispatches']:

    #         cliente_name = next((item["name"] for item in despachos["groups"] if item["group_category"] == "Cliente"), None)

    #         if "paris" in cliente_name.lower():

    #             if despachos["is_trunk"] == True:

    #                 troncales.append(despachos["is_trunk"])
    #             else :

    #                 troncales.append(despachos["is_trunk"])

    #                 # body = despachos

    #                 body = {
    #                 "identifier": despachos['identifier'],
    #                 "status_id": 1,
    #                 "substatus": None
    #                 }

    #                 # info_despachos.append(body)

    #                 info_despachos.append(body)

        
    #     print(info_despachos,troncales)

              
    #     return info_despachos, troncales

    #     # body = response.json()

    # else:
    #     print(f"Error en la solicitud GET: {response.status_code}")
    #     print(response.text)

    #     return info_despachos, troncales



def verificar_si_ruta_paris_existe_despachos(ruta_id,ruta_id_yanez):

     # URL del endpoint
    url = f'https://paris.dispatchtrack.com/api/external/v1/routes/{ruta_id}'

    # Encabezados
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Thunder Client (https://www.thunderclient.com)',
        'X-AUTH-TOKEN': config("SECRET_KEY_PARIS"),
    }

    # Realizamos la solicitud PUT
    with httpx.Client() as client:
        response = client.get(url, headers=headers)

    
    troncales = []

    info_despachos = []

    # Verificamos la respuesta
    if response.status_code == 200:
        # print("Solicitud GET exitosa:", response.json())

        data = response.json()

        
        for despachos in data['response']['route']['dispatches']:

            if despachos["is_trunk"] == True:

                troncales.append(despachos["is_trunk"])
            else:

                troncales.append(despachos["is_trunk"])

                body = {
                    "identifier": despachos['identifier']
                }

                info_despachos.append(body)

        
        return info_despachos, troncales

        # body = response.json()

    else:
        print(f"Error en la solicitud GET: {response.status_code}")
        print(response.text)

        return info_despachos, troncales



def verificar_si_ruta_paris_existe(ruta_id):

     # URL del endpoint
    url = f'https://paris.dispatchtrack.com/api/external/v1/routes/{ruta_id}'

    # Encabezados
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Thunder Client (https://www.thunderclient.com)',
        'X-AUTH-TOKEN': config("SECRET_KEY_PARIS"),
    }

    # Realizamos la solicitud PUT
    with httpx.Client() as client:
        response = client.get(url, headers=headers)

    # Verificamos la respuesta
    if response.status_code == 200:
        print("Solicitud GET exitosa:", response.json())

        return ruta_id

        # body = response.json()

    else:
        print(f"Error en la solicitud GET: {response.status_code}")
        print(response.text)

        return None


def obtener_info_despacho(distpach):

     # URL del endpoint
    url = f'https://paris.dispatchtrack.com/api/external/v1/dispatches/{distpach}?evaluations=true'

    # Encabezados
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Thunder Client (https://www.thunderclient.com)',
        'X-AUTH-TOKEN': config("SECRET_KEY_PARIS"),
    }

    # Realizamos la solicitud PUT
    with httpx.Client() as client:
        response = client.get(url, headers=headers)

    # Verificamos la respuesta
    if response.status_code == 200:

        data = response.json()
        # print("Solicitud GET exitosa:", response.json())
        route_id = data["response"]["route_id"]

        return route_id

        # body = response.json()

    else:
        print(f"Error en la solicitud GET: {response.status_code}")
        print(response.text)

        return None

##### PARIS


def construct_body_from_actualizacion_guia(actualizacion_guia :ActualizacionGuia, itemNumber : int):
    # Crear un diccionario que contiene los datos necesarios para la inserción
    body = {
        "guide": actualizacion_guia.guide,
        "identifier": actualizacion_guia.identifier,
        "route_id": actualizacion_guia.route_id,
        "dispatch_id": actualizacion_guia.dispatch_id,
        "truck_identifier": actualizacion_guia.truck_identifier,
        "contact_name": actualizacion_guia.contact_name,
        "contact_phone": actualizacion_guia.contact_phone,
        "contact_identifier": actualizacion_guia.contact_identifier,
        "contact_email": actualizacion_guia.contact_email,
        "contact_address": actualizacion_guia.contact_address,

        # Asegúrate de que los tags sean manejados apropiadamente, si tienes múltiples tags

        "tag_asn_id": [tag.value for tag in actualizacion_guia.tags if tag.name == 'ASN_ID'][0]  if actualizacion_guia.tags else None,
        "tag_desc_comuna": [tag.value for tag in actualizacion_guia.tags if tag.name == 'Desc_Comuna'][0]  if actualizacion_guia.tags else None,
        "tag_desc_emp": [tag.value for tag in actualizacion_guia.tags if tag.name == 'DESC_EMP'][0]  if actualizacion_guia.tags else None,
        "tag_do_id": [tag.value for tag in actualizacion_guia.tags if tag.name == 'DO_ID'][0]  if actualizacion_guia.tags else None,
        "tag_fecdesfis": [tag.value for tag in actualizacion_guia.tags if tag.name == 'FECDESFIS'][0]  if actualizacion_guia.tags else None,
        "fechaemi": [tag.value for tag in actualizacion_guia.tags if tag.name == 'FECHAEMI'][0]  if actualizacion_guia.tags else None,
        "fecsoldes": [tag.value for tag in actualizacion_guia.tags if tag.name == 'FECSOLDES'][0]  if actualizacion_guia.tags else None,
        "numcorhr": [tag.value for tag in actualizacion_guia.tags if tag.name == 'NUMCORHR'][0]  if actualizacion_guia.tags else None,
        "numsolgui": [tag.value for tag in actualizacion_guia.tags if tag.name == 'NUMSOLGUI'][0]  if actualizacion_guia.tags else None,
        "urlcarga": [tag.value for tag in actualizacion_guia.tags if tag.name == 'URLCARGA'][0]  if actualizacion_guia.tags else None,
        "urlguia": [tag.value for tag in actualizacion_guia.tags if tag.name == 'URLGUIA'][0]  if actualizacion_guia.tags else None,


        # Los valores de los items (por ejemplo, insertarlos como el primer item)
        "item_id": actualizacion_guia.items[itemNumber].id if actualizacion_guia.items else None,
        "item_name": actualizacion_guia.items[itemNumber].name if actualizacion_guia.items else None,
        "item_description": actualizacion_guia.items[itemNumber].description if actualizacion_guia.items else None,
        "item_quantity": actualizacion_guia.items[itemNumber].quantity if actualizacion_guia.items else None,
        "item_original_quantity": actualizacion_guia.items[itemNumber].original_quantity if actualizacion_guia.items else None,
        "item_delivered_quantity": actualizacion_guia.items[itemNumber].delivered_quantity if actualizacion_guia.items else None,
        "item_code": actualizacion_guia.items[itemNumber].code if actualizacion_guia.items else None,

        "item_carton": [extra.value for extra in actualizacion_guia.items[itemNumber].extras if extra.name == 'CARTONID'][0]  if actualizacion_guia.items[itemNumber].extras else None,
        "item_sku": [extra.value for extra in actualizacion_guia.items[itemNumber].extras if extra.name == 'SKU'][0]  if actualizacion_guia.items[itemNumber].extras else None
    }

    return body



@app.post("/api/v2/dispatch")
async def webhook_dispatch_paris(request : Request , headers: tuple = Depends(validar_encabezados)):

    body = await request.json()  # Obtener el cuerpo como JSON

    folder = 'resp_api_paris_productos'

    try:

        if body["resource"] == "dispatch_guide":
            mensaje = "Recibido Modelo Creación Guia"
            data = CreacionGuia(**body)



        if body["resource"] == "dispatch":
            mensaje = "Recibido Modelo Actualización Guia"
            data = ActualizacionGuia(**body)

            lista_cartones = conn.get_cartones_despacho_paris(data.guide)[0]

            # print (data.route_id)
            if lista_cartones is None:
                lista_cartones = []

            for n in range(len(data.items)):
                carton = [extra.value for extra in data.items[n].extras if extra.name == 'CARTONID'][0]
                # print(carton)
                if carton not in lista_cartones or carton == "RETORNO":

                    ingreso = construct_body_from_actualizacion_guia(data,n)
                    conn.insert_dispatch_paris(ingreso)
                    lista_cartones.append(carton)

                    
                    body = await request.json() 
                        # Generar nombre de archivo único usando timestamp
                    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                    filename = os.path.join(folder, f"datos_nuevos_{timestamp}.txt")

                    # Guardar el contenido del JSON en un archivo de texto
                    with open(filename, "w") as f:
                        json.dump(body, f, indent=4)

                else:
                    print('carton ya existe', carton)


                    if data.status is None:
                        data.status = 0

                    if data.substatus_code is None:
                        data.substatus_code = 0


                    
                    
                    body = await request.json() 
                        # Generar nombre de archivo único usando timestamp
                    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                    filename = os.path.join(folder, f"datos_actualizado_{timestamp}.txt")

                    # Guardar el contenido del JSON en un archivo de texto
                    with open(filename, "w") as f:
                        json.dump(body, f, indent=4)


                    # conn.update_estado_dispatch_paris(data.dispatch_id, data.status,data.substatus_code)

                    # body_estados = get_update_estados_paris_yanez(data.guide) ## esto es beetrack paris Yanez

                    # body = conn.read_estados_paris(body_estados['status_id'],body_estados['substatus_code'],data.is_trunk)

                    # send_put_request(body, data.guide)



                    # send_put_request_paris_yanez(body, data.guide)



        if body["resource"] == "route":
            mensaje = "Recibido Modelo Creación Ruta"
            data = CreacionRuta(**body)

            if  data.event == 'create':
                print('crear ruta')
                conn.insert_creacion_ruta_paris(data.dict())

            if data.event != 'create':
                print('actualizar ruta')
                row = conn.update_ruta_paris(data.dict())
                print(row)




        return {
                "message": mensaje
                # "datos": data
                }
    except Exception as error:


        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = os.path.join(folder, f"datos_por_error_400_{timestamp}.txt")

        # Guardar el contenido del JSON en un archivo de texto
        with open(filename, "w") as f:
            json.dump(body, f, indent=4)

            f.write(" ")
            f.write(str(error))

        print('Error al recibir el cuerpo del mensaje de dispatch paris',error)
        raise HTTPException(status_code=400, detail="Error al recibir el cuerpo del mensaje")



@app.post("/api/v2/dispatch/yanez")
async def webhook_dispatch_yanez(request : Request , headers: tuple = Depends(validar_encabezados)):

    body = await request.json()  # Obtener el cuerpo como JSON

    date_actual = datetime.now().strftime("%Y-%m-%d")

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"datos_de_transyanez_{timestamp}.txt"


    # body = await request.json()  # Obtener el cuerpo como JSON
    # Guardar el contenido del JSON en un archivo de texto
    with open(filename, "w") as f:
        json.dump(body, f, indent=4)

    

    try:

        if body["resource"] == "dispatch_guide":
            mensaje = "Recibido Modelo Creación Guia"
            data = CreacionGuia(**body)

        if body["resource"] == "dispatch":
            mensaje = "Recibido Modelo Actualización Guia"
            data = ActualizacionGuia(**body)
            body_estados = None

        return {
                "message": mensaje
                # "datos": data
                }
    except Exception as error:


        print(error)

        body = await request.json()  # Obtener el cuerpo como JSON

        # timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # filename = f"datos_wh_bt_yanez__error_400_{timestamp}.txt"

        # # Guardar el contenido del JSON en un archivo de texto
        # with open(filename, "w") as f:
        #     f.write(str(error))

        print('Error al recibir el cuerpo del mensaje de dispatch paris',error)
        raise HTTPException(status_code=400, detail="Error al recibir el cuerpo del mensaje")



@app.get("/api/v2/dispatch/test")
async def post_dispatch_guide(dispatch_id :int):

    lista_cartones = conn.get_cartones_despacho_paris(dispatch_id)

    return {
            "body" : lista_cartones[0]
            }


def convertir_items_a_formato(item : Item, datos_obtenidos):

    if datos_obtenidos is None:
        return {
            "description": item.description,
            "code": item.code,
            # "quantity": item.quantity,
            # "quantity_ref": item.delivered_quantity,
            "quantity": item.quantity,
            "delivered_quantity": item.delivered_quantity,
        } 

    item_code = next((item_obtenido["item_code"] for item_obtenido in datos_obtenidos if item_obtenido["item_carton"] == item.code), None)


    return {
            "description": item.description,
            "code": item_code,
            # "quantity": item.quantity,
            # "quantity_ref": item.delivered_quantity,
            "quantity": item.quantity,
            "delivered_quantity": item.delivered_quantity,
        } 



@app.post("/api/v2/dispatch/paris/actualizacion")
async def post_dispatch_guide(request : Request , headers: tuple = Depends(validar_encabezados)):

        body = await request.json()  # Obtener el cuerpo como JSON

        date_actual = datetime.now().strftime("%Y-%m-%d")

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        folder = "dispatch_guide"


        filename = os.path.join(folder, f"datos_ruta_parasabersillegan_ACTUALIZACION{timestamp}.txt")

        with open(filename, "w") as f:
            json.dump(body, f, indent=4)

        # try:


        if body["resource"] == "route":
            mensaje = "Recibido Modelo Creación Ruta"
            data = CreacionRuta(**body)

            folder = "route"

            # Asegúrate de que la carpeta exista
            os.makedirs(folder, exist_ok=True)

            filename = os.path.join(folder, f"datos_ruta_{data.route}_{timestamp}.txt")

            with open(filename, "w") as f:
                json.dump(body, f, indent=4)


            ruta_paris = conn.verificar_informacion_ruta_paris(data.route)

            ### Primer filtro: verificar si la ruta existe en paris

            if ruta_paris is None:
                print('no existe la ruta en paris')
                return { "message": "no existe la ruta en paris"}
            


            if data.event == 'update' and data.started == True:
                
                print('actualizar ruta existente')
                estado_ruta = ruta_paris[2]

                if estado_ruta == 'started':
                    print('la ruta ya esta iniciada')
                    return { "message": "la ruta ya esta iniciada"}

                

                # despachos, troncales = verificar_si_ruta_paris_existe_despachos(verificar_info_ruta[1], verificar_info_ruta[0])
                despachos, troncales = verificar_si_ruta_yanez_existe_despachos(ruta_paris[0])

                if not despachos and not troncales:

                    print("No hay despachos ni troncales")

                    return { "message": "no hay despachos ni troncales"}

                if any(troncales):
                    print("Al menos un troncal es True")
                    pass
                else:
                    print("Todos son False")

                    fecha_formateada = datetime.now() - timedelta(minutes=3)

                    # Formatear con milisegundos .000 y zona horaria fija -04:00
                    fecha_actual = fecha_formateada.strftime("%Y-%m-%dT%H:%M:%S.000-04:00")

                    if despachos is None:
                        return { "message": "esta ruta no se puede iniciar"}


                    body_put_request = {
                        "id": ruta_paris[1],
                        # "started": True,
                        "started_at": fecha_actual
                        ,
                        "dispatches": despachos                  
                    }

                    conn.actualizar_estado_ruta_paris(ruta_paris[0],'started')

                    # print(body_put_request)
                    # print(despachos)

                    send_put_update_ruta(body_put_request, ruta_paris[1])


                    return { "message": "esta ruta es iniciada"}


            if data.event == 'finish':

                despachos, troncales = verificar_si_ruta_paris_existe_despachos(ruta_paris[1], ruta_paris[0])


                if not despachos and not troncales:
                    print("NO HAY QUEEEEEEEEEEEEEEEESOOOO")


                if any(troncales):
                    print("Al menos un troncal es True")
                    pass
                else:
                    print("Todos son False")

                    body_put_request = {
                        "ended": True
                    }

                    conn.actualizar_estado_ruta_paris(ruta_paris[0],'finished')

                    send_put_update_ruta(body_put_request, ruta_paris[1])



            else:

                    print('no pasa nada')

        if body["resource"] == "dispatch_guide":
            mensaje = "Recibido Modelo Creación Guia"
            data = CreacionGuia(**body)

            folder = "dispatch_guide"

            # Asegúrate de que la carpeta exista
            os.makedirs(folder, exist_ok=True)

            filename = os.path.join(folder, f"datos_guia_despacho_{data.dispatch_guide.guide}_{timestamp}.txt")

            with open(filename, "w") as f:
                json.dump(body, f, indent=4)


        if body["resource"] == "dispatch":
            
            mensaje = "Recibido Modelo Actualización Guia"
            data = ActualizacionGuia(**body)

            folder = "dispatch"

            # Asegúrate de que la carpeta exista
            os.makedirs(folder, exist_ok=True)
            
            filename = os.path.join(folder, f"datos_despacho_{data.dispatch_id}_{timestamp}.txt")

            with open(filename, "w",encoding="utf-8") as f:
                json.dump(body, f, indent=4,ensure_ascii=False)

            ### aqui se empieza a hacer la logica de actualizacion de guia en dispatchtrack paris

            #### para empezar se confirma si el dispatch pertenece a paris

            # Obtener el name del group_category "Cliente"
            cliente_name = next((item["name"] for item in data.groups if item["group_category"] == "Cliente"), None)

            # "paris" in datos_groups["Cliente"].lower()
            if "paris" in cliente_name.lower():
                print("El dispatch pertenece a Paris")
            else:
                return {"message": "El dispatch no pertenece a Paris"}



            body_estados = None

            if data.route_id is None:
                verificar_info_ruta = None
            else:
                print('id_ty',data.route_id)
                verificar_info_ruta = conn.verificar_informacion_ruta_paris(data.route_id)

            # print(data.waypoint)

            if data.waypoint is not None:
                latitude = data.waypoint.latitude
                longitude = data.waypoint.longitude
            else:
                latitude = ""
                longitude = ""

            if data.is_trunk is None:
                data.is_trunk = False

            if data.status is None:
                data.status = 0

            if data.substatus_code is None:
                data.substatus_code = "null"

                
            body_estados = conn.read_estados_paris(data.status,data.substatus_code, data.is_trunk,latitude,longitude)

            if body_estados is None:
                body_estados = [1,None]

            print('body_estados', body_estados)

            if data.is_trunk == True: ## si el troncal viene como true, entonces se crea la ruta en paris
                print("trunk : true")
                # id_ruta_creada = conn.recuperar_ruta_registrada_paris(data.guide)[0]

                #####  primero hay que verificar la ruta de paris, si existe o no existe
                #### en base a lo que reciba de la tabla de ppu_tracking

                print(verificar_info_ruta)


                if data.substatus_code == None or data.substatus_code == "null" :### si el substatus es nulo, entonces no se actualiza nada
                    print("es substatus es nulo")
                else: ##### se usa el metodo put para update de rutas
                    ##### body update ruta

                    id_ruta_creada = obtener_info_despacho(data.identifier)

                    body_put_request = {
                        "id": id_ruta_creada,
                        "dispatches": 
                            [{
                            "identifier": data.identifier,
                            "status_id": body_estados[0],
                            # "substatus": body_estados[1],
                            "place": "CT Transyañez",
                            "is_trunk":  data.is_trunk,
                            "waypoint": {
                                "latitude": latitude,
                                "longitude": longitude
                            }
                        }]
                        }
                    
                    # print(body_put_request)
                
                    send_put_update_ruta(body_put_request, id_ruta_creada)


            else: ## si el troncal viene como false, entonces se actualiza de la forma 
                print('troncal : false')

                # print(verificar_info_ruta)

                ##### logica para iniciar ruta 

                print("logica para iniciar ruta")
                print(data.status)
                print(data.substatus_code)


                ######## lo que se hace es verificar si la ruta existe o no existe en dispatchtrack paris

                if verificar_info_ruta is None:
                    print('la ruta no existe en el dispatchtrack paris')

                    ######  print('se debe crear vehiculo en paris')
                    crear_vehiculo_paris(data.truck_identifier)  
                    
                    date_actual = datetime.now().strftime("%Y-%m-%d")

                    ### luego se crea la ruta en paris
                    body_ruta = {
                        "truck_identifier":data.truck_identifier,
                        "date": date_actual,
                        "dispatches": [{"identifier": data.identifier}]
                    }

                    # if data.route_id is None:
                    id_ruta_creada = crear_ruta_paris(body_ruta)
                    print(' rUTA NUEVA')
                    time.sleep(0.8)

                    body_info_ruta = {
                    "ppu" : data.truck_identifier, 
                    "id_route_ty" : data.route_id, 
                    "id_route_paris" : id_ruta_creada, 
                    "is_trunk" : True
                    }

                    # print(body_info_ruta)
            
                    conn.guardar_informacion_de_rutas_paris(body_info_ruta)


                    return { "message": "la ruta no existe en dispatchtrack paris"}

                else:
                    ### se hacce la actualizacion de la ruta existente
                    url_img =  []

                    if data.evaluation_answers is not None:

                        for imagen in data.evaluation_answers:
                            if imagen.cast == "photo":
                               url_img.append(imagen.value)


                        cartones = [item.code for item in data.items]
                        result = ",".join(f"'{code}'" for code in cartones)

                        datos_obtenidos = conn.obtener_codigos_cartones_paris(result)[0]


                        itemes = [ convertir_items_a_formato(item,datos_obtenidos) for item in data.items]
                        # print("itemes")

                        # print(itemes)

                       
                        if url_img == []:
                            body = {
                                        "id": verificar_info_ruta[1],
                                        "dispatches": 
                                            [{
                                            "identifier": data.identifier,
                                            "status_id": body_estados[0],
                                            "substatus": body_estados[1],
                                            # "place": "CT Transyañez",
                                            "is_trunk":  data.is_trunk,
                                            "items": itemes,
                                            "waypoint": {
                                                "latitude": latitude,
                                                "longitude": longitude
                                            }
                                            }]
                                        }
                            
                            #### esta condición es para las entregas parciales sin imagenes
                            if data.status == 4:
                                body = {
                                        "id": verificar_info_ruta[1],
                                        "dispatches": 
                                            [{
                                            "identifier": data.identifier,
                                            "status": body_estados[0],
                                            # "substatus": body_estados[1],
                                            "substatus_code": int(body_estados[2]),
                                            # "is_pickup": True,
                                            "is_trunk":  data.is_trunk,
                                            "items": itemes,
                                            "waypoint": {
                                                "latitude": latitude,
                                                "longitude": longitude
                                            }
                                            }]
                                        }
                                
                                send_put_update_ruta(body,verificar_info_ruta[1])

                                return {
                                    "message": "actualizar ruta existente"
                                }

                        else:
                            body = {
                                        "id": verificar_info_ruta[1],
                                        "dispatches": 
                                            [{
                                            "identifier": data.identifier,
                                            "status_id": body_estados[0],
                                            "substatus": body_estados[1],
                                            # "place": "CT Transyañez",
                                            "items": itemes,
                                            "is_trunk":  data.is_trunk,
                                            "waypoint": {
                                                "latitude": latitude,
                                                "longitude": longitude
                                            },
                                            "form":{
                                                "img_url": url_img
                                                    }
                                            }]
                                        }

                            
                            #### esta condición es para las entregas parciales con imagenes
                            if data.status == 4:
                                body = {
                                        "id": verificar_info_ruta[1],
                                        "dispatches": 
                                            [{
                                            "identifier": data.identifier,
                                            "status": body_estados[0],
                                            # "substatus": body_estados[1],
                                            "substatus_code":  int(body_estados[2]),
                                            # "is_pickup": True,
                                            # "place": "CT Transyañez",
                                            "is_trunk":  data.is_trunk,
                                            "items": itemes,
                                            "waypoint": {
                                                "latitude": latitude,
                                                "longitude": longitude
                                            },
                                            "form":{
                                                "img_url": url_img
                                                    }
                                            }]
                                        }
                                
                                # print(body)
                                
                                send_put_update_ruta(body,verificar_info_ruta[1])

                                return {
                                    "message": "actualizar ruta existente"
                                }
                    else:
                        body = {
                                    "id": verificar_info_ruta[1],
                                    "dispatches": 
                                        [{
                                        "identifier": data.identifier,
                                        "status_id": body_estados[0],
                                        "substatus": body_estados[1],
                                        # "place": "CT Transyañez",
                                        "is_trunk":  data.is_trunk,
                                        "waypoint": {
                                            "latitude": latitude,
                                            "longitude": longitude
                                        }

                                        }]
                                    }

                        
                    # print(body)


                    if data.substatus_code == "45" or data.substatus_code == "46":
                         

                         print('substatus 45 o 46, no se envia nada')
                        #  body = {
                        #             "id": verificar_info_ruta[1],
                        #             "dispatches": 
                        #                 [{
                        #                 "identifier": data.identifier,
                        #                 "status_id": body_estados[0],
                        #                 "substatus": body_estados[1],
                        #                 "place": "CT Transyañez",
                        #                 "is_trunk":  True,
                        #                 "waypoint": {
                        #                     "latitude": latitude,
                        #                     "longitude": longitude
                        #                 }
                        #                 }]
                        #             }
                    else:
      
                        print('actualizar ruta existente')
                        send_put_update_ruta(body,verificar_info_ruta[1])
                        # print('codigos carton')
                        

                        ### esto se comenta para que no se actualice la ruta en dispatchtrack paris
                         


                pass

        return {
                "message": mensaje
                # "datos": data
                }
    # except Exception as error:

    #     print('Error al recibir el cuerpo del mensaje de dispatch paris',error)
    #     raise HTTPException(status_code=400, detail="Error al recibir el cuerpo del mensaje")
    


########## FUNCION PARA LOS DISPATCH PARIS

def procesar_datos_despachos_paris(body):

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    if body["resource"] == "dispatch":
            
            mensaje = "Recibido Modelo Actualización Guia"
            data = ActualizacionGuia(**body)

            folder = "dispatch"

            # Asegúrate de que la carpeta exista
            os.makedirs(folder, exist_ok=True)
            
            filename = os.path.join(folder, f"datos_despacho_{data.dispatch_id}_{timestamp}.txt")

            with open(filename, "w",encoding="utf-8") as f:
                json.dump(body, f, indent=4,ensure_ascii=False)

            ### aqui se empieza a hacer la logica de actualizacion de guia en dispatchtrack paris

            #### para empezar se confirma si el dispatch pertenece a paris

            # Obtener el name del group_category "Cliente"
            cliente_name = next((item["name"] for item in data.groups if item["group_category"] == "Cliente"), None)

            # "paris" in datos_groups["Cliente"].lower()
            if "paris" in cliente_name.lower():
                print("El dispatch pertenece a Paris")
            else:
                return {"message": "El dispatch no pertenece a Paris"}



            body_estados = None

            if data.route_id is None:
                verificar_info_ruta = None
            else:
                print('id_ty',data.route_id)
                verificar_info_ruta = conn.verificar_informacion_ruta_paris(data.route_id)

            # print(data.waypoint)

            if data.waypoint is not None:
                latitude = data.waypoint.latitude
                longitude = data.waypoint.longitude
            else:
                latitude = ""
                longitude = ""

            if data.is_trunk is None:
                data.is_trunk = False

            if data.status is None:
                data.status = 0

            if data.substatus_code is None:
                data.substatus_code = "null"

                
            body_estados = conn.read_estados_paris(data.status,data.substatus_code, data.is_trunk,latitude,longitude)

            if body_estados is None:
                body_estados = [1,None]

            print('body_estados', body_estados)

            if data.is_trunk == True: ## si el troncal viene como true, entonces se crea la ruta en paris
                print("trunk : true")
                # id_ruta_creada = conn.recuperar_ruta_registrada_paris(data.guide)[0]

                #####  primero hay que verificar la ruta de paris, si existe o no existe
                #### en base a lo que reciba de la tabla de ppu_tracking

                # print(verificar_info_ruta)


                if data.substatus_code == None or data.substatus_code == "null" :### si el substatus es nulo, entonces no se actualiza nada
                    print("es substatus es nulo")
                else: ##### se usa el metodo put para update de rutas
                    ##### body update ruta

                    id_ruta_creada = obtener_info_despacho(data.identifier)

                    body_put_request = {
                        "id": id_ruta_creada,
                        "dispatches": 
                            [{
                            "identifier": data.identifier,
                            "status_id": body_estados[0],
                            # "substatus": body_estados[1],
                            "place": "CT Transyañez",
                            "is_trunk":  data.is_trunk,
                            "waypoint": {
                                "latitude": latitude,
                                "longitude": longitude
                            }
                        }]
                        }
                    
                    # print(body_put_request)
                
                    send_put_update_ruta(body_put_request, id_ruta_creada)


            else: ## si el troncal viene como false, entonces se actualiza de la forma 
                print('troncal : false')

                # print(verificar_info_ruta)

                ##### logica para iniciar ruta 

                # print("logica para iniciar ruta")
                # print(data.status)
                # print(data.substatus_code)


                ######## lo que se hace es verificar si la ruta existe o no existe en dispatchtrack paris

                if verificar_info_ruta is None:
                    print('la ruta no existe en el dispatchtrack paris')


                    if data.truck_identifier is None:
                        print('el vehiculo es nulo')
                        return { "message": "no existe el vehiculo en dispatchtrack paris"}

                    ######  print('se debe crear vehiculo en paris')
                    crear_vehiculo_paris(data.truck_identifier)  
                    
                    date_actual = datetime.now().strftime("%Y-%m-%d")

                    ### luego se crea la ruta en paris
                    body_ruta = {
                        "truck_identifier":data.truck_identifier,
                        "date": date_actual,
                        "dispatches": [{"identifier": data.identifier}]
                    }

                    # if data.route_id is None:
                    id_ruta_creada = crear_ruta_paris(body_ruta)
                    print(' rUTA NUEVA')
                    time.sleep(0.8)

                    body_info_ruta = {
                    "ppu" : data.truck_identifier, 
                    "id_route_ty" : data.route_id, 
                    "id_route_paris" : id_ruta_creada, 
                    "is_trunk" : True
                    }

                    # print(body_info_ruta)
            
                    conn.guardar_informacion_de_rutas_paris(body_info_ruta)


                    return { "message": "la ruta no existe en dispatchtrack paris"}

                else:
                    ### se hacce la actualizacion de la ruta existente
                    url_img =  []

                    if data.evaluation_answers is not None:

                        for imagen in data.evaluation_answers:
                            if imagen.cast == "photo":
                               url_img.append(imagen.value)


                        cartones = [item.code for item in data.items]
                        result = ",".join(f"'{code}'" for code in cartones)

                        datos_obtenidos = conn.obtener_codigos_cartones_paris(result)[0]


                        itemes = [ convertir_items_a_formato(item,datos_obtenidos) for item in data.items]
                        # print("itemes")

                        # print(itemes)

                       
                        if url_img == []:
                            body = {
                                        "id": verificar_info_ruta[1],
                                        "dispatches": 
                                            [{
                                            "identifier": data.identifier,
                                            "status_id": body_estados[0],
                                            "substatus": body_estados[1],
                                            # "place": "CT Transyañez",
                                            "is_trunk":  data.is_trunk,
                                            "items": itemes,
                                            "waypoint": {
                                                "latitude": latitude,
                                                "longitude": longitude
                                            }
                                            }]
                                        }
                            
                            #### esta condición es para las entregas parciales sin imagenes
                            if data.status == 4:
                                body = {
                                        "id": verificar_info_ruta[1],
                                        "dispatches": 
                                            [{
                                            "identifier": data.identifier,
                                            "status": body_estados[0],
                                            # "substatus": body_estados[1],
                                            "substatus_code": int(body_estados[2]),
                                            # "is_pickup": True,
                                            "is_trunk":  data.is_trunk,
                                            "items": itemes,
                                            "waypoint": {
                                                "latitude": latitude,
                                                "longitude": longitude
                                            }
                                            }]
                                        }
                                
                                send_put_update_ruta(body,verificar_info_ruta[1])

                                return {
                                    "message": "actualizar ruta existente"
                                }

                        else:
                            body = {
                                        "id": verificar_info_ruta[1],
                                        "dispatches": 
                                            [{
                                            "identifier": data.identifier,
                                            "status_id": body_estados[0],
                                            "substatus": body_estados[1],
                                            # "place": "CT Transyañez",
                                            "items": itemes,
                                            "is_trunk":  data.is_trunk,
                                            "waypoint": {
                                                "latitude": latitude,
                                                "longitude": longitude
                                            },
                                            "form":{
                                                "img_url": url_img
                                                    }
                                            }]
                                        }
                            
                            
                            
                            #### esta condición es para las entregas parciales con imagenes
                            if data.status == 4:
                                body = {
                                        "id": verificar_info_ruta[1],
                                        "dispatches": 
                                            [{
                                            "identifier": data.identifier,
                                            "status": body_estados[0],
                                            # "substatus": body_estados[1],
                                            "substatus_code":  int(body_estados[2]),
                                            # "is_pickup": True,
                                            # "place": "CT Transyañez",
                                            "is_trunk":  data.is_trunk,
                                            "items": itemes,
                                            "waypoint": {
                                                "latitude": latitude,
                                                "longitude": longitude
                                            },
                                            "form":{
                                                "img_url": url_img
                                                    }
                                            }]
                                        }
                                
                                # print(body)
                                
                                send_put_update_ruta(body,verificar_info_ruta[1])

                                return {
                                    "message": "actualizar ruta existente"
                                }
                    else:
                        body = {
                                    "id": verificar_info_ruta[1],
                                    "dispatches": 
                                        [{
                                        "identifier": data.identifier,
                                        "status_id": body_estados[0],
                                        "substatus": body_estados[1],
                                        # "place": "CT Transyañez",
                                        "is_trunk":  data.is_trunk,
                                        "waypoint": {
                                            "latitude": latitude,
                                            "longitude": longitude
                                        }

                                        }]
                                    }

                        
                    # print(body)


                    if data.substatus_code == "45" or data.substatus_code == "46":
                         

                         print('substatus 45 o 46, no se envia nada')
                        #  body = {
                        #             "id": verificar_info_ruta[1],
                        #             "dispatches": 
                        #                 [{
                        #                 "identifier": data.identifier,
                        #                 "status_id": body_estados[0],
                        #                 "substatus": body_estados[1],
                        #                 "place": "CT Transyañez",
                        #                 "is_trunk":  True,
                        #                 "waypoint": {
                        #                     "latitude": latitude,
                        #                     "longitude": longitude
                        #                 }
                        #                 }]
                        #             }
                    else:
      
                        # print('actualizar ruta existente')
                        send_put_update_ruta(body,verificar_info_ruta[1])

                pass
    
    
    return True


##### FUNCION PARA LAS RUTAS QUE PERTENECEN A PARIS

def procesar_datos_rutas_paris(body):

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    if body["resource"] == "route":
            mensaje = "Recibido Modelo Creación Ruta"
            data = CreacionRuta(**body)

            folder = "route"

            # Asegúrate de que la carpeta exista
            os.makedirs(folder, exist_ok=True)

            filename = os.path.join(folder, f"datos_ruta_{data.route}_{timestamp}.txt")

            with open(filename, "w") as f:
                json.dump(body, f, indent=4)


            ruta_paris = conn.verificar_informacion_ruta_paris(data.route)

            ### Primer filtro: verificar si la ruta existe en paris

            if ruta_paris is None:
                # print('no existe la ruta en paris')
                return { "message": "no existe la ruta en paris"}
            


            if data.event == 'update' and data.started == True and data.ended == False:
                
                print('actualizar ruta existente')
                estado_ruta = ruta_paris[2]

                if estado_ruta == 'started':
                    # print('la ruta ya esta iniciada')
                    return { "message": "la ruta ya esta iniciada"}

                

                # despachos, troncales = verificar_si_ruta_paris_existe_despachos(verificar_info_ruta[1], verificar_info_ruta[0])
                despachos, troncales = verificar_si_ruta_yanez_existe_despachos(ruta_paris[0])

                if not despachos and not troncales:

                    # print("No hay despachos ni troncales")

                    return { "message": "no hay despachos ni troncales"}

                if any(troncales):
                    # print("Al menos un troncal es True")
                    pass
                else:
                    # print("Todos son False")

                    fecha_formateada = datetime.now() - timedelta(minutes=3)

                    # Formatear con milisegundos .000 y zona horaria fija -04:00
                    fecha_actual = fecha_formateada.strftime("%Y-%m-%dT%H:%M:%S.000-04:00")


                    body_put_request = {
                        "id": ruta_paris[1],
                        # "started": True,
                        "started_at": fecha_actual,
                        "dispatches": despachos                  
                    }

                    conn.actualizar_estado_ruta_paris(ruta_paris[0],'started')

                    print(body_put_request)
                    print(despachos)

                    send_put_update_ruta(body_put_request, ruta_paris[1])


                    return { "message": "esta ruta es iniciada"}


            if data.event == 'finish':

                despachos, troncales = verificar_si_ruta_paris_existe_despachos(ruta_paris[1], ruta_paris[0])


                if not despachos and not troncales:
                    print("NO HAY QUEEEEEEEEEEEEEEEESOOOO")


                if any(troncales):
                    print("Al menos un troncal es True")
                    pass
                else:
                    print("Todos son False")

                    body_put_request = {
                        "ended": True
                    }

                    conn.actualizar_estado_ruta_paris(ruta_paris[0],'finished')

                    send_put_update_ruta(body_put_request, ruta_paris[1])

            else:

                    print('no pasa nada')



@app.post("/api/v2/beetrack/dispatch")
# async def post_dispatch(body : Dispatch, headers: tuple = Depends(validar_encabezados)):
async def post_dispatch(request : Request, headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers
    # Lista de nombres que deseas buscar
    body_p = await request.json()  # Obtener el cuerpo como JSON

    body = Dispatch(**body_p)

    data = body.dict()

    # date_actual = datetime.now().strftime("%Y-%m-%d")

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    folder = "dispatch_guide"


    filename = os.path.join(folder, f"datos_ruta_parasabersillegan_ORIGINAL{timestamp}.txt")

    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

    ##### logica para las route

    

    if data["resource"] == 'route' and data["event"] == 'create':

        datos_insert_ruta = data_beetrack.generar_data_insert_creacion_ruta(data)
        conn.insert_beetrack_creacion_ruta(datos_insert_ruta)


    if data["resource"] == 'route' and data["event"] == 'update' and data["started"] == True:
        procesar_datos_rutas_paris(body_p)


    if data["resource"] == 'route' and data["event"] == 'finish':
        procesar_datos_rutas_paris(body_p)

    if data["resource"] == 'route' and data["event"] in ['start', 'finish']:
        datos_insert_ruta = data_beetrack.generar_data_insert_creacion_ruta(data)
        row = conn.update_route_beetrack_event(datos_insert_ruta)

        return {
            "message" : "data recibida correctamente"
            }
    

    ##### logica para los despachos

    if data["resource"] == 'dispatch' and data["event"] == 'update':
        datos_create = {
                        "ruta_id" : data["route_id"],
                        "guia" : data["guide"]
                        }
        
        resultado = conn.verificar_si_ruta_existe(datos_create)

        if len(resultado) == 0:
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

            if "paris" in datos_groups_i["Cliente"].lower():


                procesar_datos_despachos_paris(body_p)

                # body = {
                #     "status": data["status"],
                #     "substatus": data["substatus"],
                #     "substatus_code": data["substatus_code"]
                # }

                # body = conn.read_estados_paris(data["status"],data["substatus_code"])

                # send_put_request(body, data["guide"])


                return {
                    "message" : "data recibida correctamente"
                }



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

            #### proceso para guardar imagenes en la base de datos

            guia_update = ActualizacionGuiaStr(**body_p)

            url_img = []

            if guia_update.evaluation_answers is not None:

                for imagen in guia_update.evaluation_answers:
                    # if imagen.cast == "photo":
                        url_img.append(imagen.value)

                if url_img == []:
                    pass
                else:

                    # Expandir y limpiar
                    flat_urls = []
                    for item in url_img:
                        urls = item.split(',')
                        flat_urls.extend([url.strip() for url in urls if url.strip()])

                    # Construir string para el UPDATE
                    sql_array = "ARRAY[\n  " + ",\n  ".join(f"'{url}'" for url in flat_urls) + "\n]"

                    respon = conn.update_fotos_entrega_ruta_ty_event(sql_array,guia_update.guide,guia_update.route_id)

            if datos_groups["Cliente"] == "Electrolux":
                patron = r'\D+'
                factura = re.sub(patron, '', datos_tags["FACTURA"])

                ahora = datetime.now()

                datos = {
                    "Numero" : factura,
                    "Hora_registro": str(ahora)
                }

                guardar_json.guardar_datos_a_archivo_existente_cf(datos,ahora,'info_factura')


            if "paris" in datos_groups["Cliente"].lower():

                # print("el cliente es paris")

                procesar_datos_despachos_paris(body_p)

                # body = conn.read_estados_paris(data["status"],data["substatus_code"])

                # body = {
                #     "status": data["status"],
                #     "substatus": data["substatus"],
                #     "substatus_code": data["substatus_code"]
                # }

                # send_put_request(body, data["guide"])

                # cliente_paris = conn.read_clientes_de_paris(dato_ruta_ty)[0]


                return {
                    "message" : "data recibida correctamente"
                }

    return {
            "message" : "data recibida correctamente"
            }


########### esto es el login migrado para no sufra por las c aidas
from database.models.user import loginSchema
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm


hela_conn = HelaConnection()
conn_user = UserConnection()

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 100

oauth2 = OAuth2PasswordBearer(tokenUrl="/login")

@app.post("/api/v2/login", status_code=status.HTTP_202_ACCEPTED)
def login_user(user_data:loginSchema):
    data = user_data.dict()
    user_db = hela_conn.read_only_one(user_data.mail.lower())
    server = "hela"

    if user_db is None:
        user_db = conn_user.read_only_one(data)
        server = "portal"
        if user_db is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="El usuario no existe")

    if not verify_password(data["password"],user_db[3]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="la contraseña no es correcta")

    if not user_db[4]:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="El usuario esta inactivo")

    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    access_token = {"sub": user_db[1],
                    "exp": expire,
                    "uid": user_db[0],
                    "email": user_db[2],
                    "active": user_db[4],
                    "rol_id":user_db[5],
                    "imagen_perfil" : user_db[6]
                    }

    return {
        "access_token": jwt.encode(access_token, config("SECRET_KEY"),algorithm=ALGORITHM),
        "token_type":"bearer",
        "rol_id" : user_db[5],
        "sub": user_db[1],
        "server": server,
        "imagen_perfil" : user_db[6]
    }

def auth_user(token:str = Depends(oauth2)):

    exception = HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="credenciales no corresponden",
                            headers={"WWW-Authenticate": "Bearer"})
    try:
        username = jwt.decode(token, key=config("SECRET_KEY"), algorithms=[ALGORITHM])
        if username is None:
            raise exception
    except JWTError:
        raise exception

    return username

def current_user(user = Depends(auth_user)):

    if not user["active"]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="usuario inactivo",
                            headers={"WWW-Authenticate": "Bearer"})
    return user



@app.get("/api/v2/user")
def me (user:TokenPayload = Depends(current_user)):

    return user



#### API para los registros de externos


@app.post("/api/v2/externo/registar/candidato")
async def Registro_candidatos_externos(body : ContactoExterno ):


    try:
        body.Nombre_contacto = body.Nombre_contacto+' '+body.Apellido

        data = body.dict()

        conn.insert_recluta_externo(data)
        return {
                'message' : 'Datos ingresados correctamente'
                }
    except psycopg2.errors.UniqueViolation as error:
        # Manejar la excepción UniqueViolation específica

        match = re.search(r"Key \(telefono\)=\((\+?\d+)\)", str(error))
        if match:
            # telefono = match.group(1)
            # Generar un error con el teléfono extraído
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: El teléfono ya se encuentra registrado")
        else:
            # Si no se encuentra el teléfono, puedes manejar el error de manera genérica
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Error: El correo ya se encuentra registrado")

    except Exception as error:
        # Manejar otras excepciones
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error al agregar al nuevo recluta.")


@app.get("/api/v2/externo/campos")
async def get_campos_registro():
    datos = conn.obtener_campos_recluta_externo()
    resultado_dict = {titulo : cant for titulo, cant in datos}

    return resultado_dict




@app.get("/api/v2/prueba/funcion")
async def get_campos_registro():
    
    resultado_dict = conn.prueba_hora()

    print(resultado_dict)

    return resultado_dict[0]
