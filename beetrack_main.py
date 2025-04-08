import json
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
from database.models.dispatch_paris.distpatch import CreacionGuia, CreacionRuta, ActualizacionGuia


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
        print("error con token")
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

@app.post("/api/v2/beetrack/dispatch")
async def post_dispatch(body : Dispatch, headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers
    # Lista de nombres que deseas buscar
    data = body.dict()
 
    if data["resource"] == 'route' and data["event"] == 'create':

        datos_insert_ruta = data_beetrack.generar_data_insert_creacion_ruta(data)
        conn.insert_beetrack_creacion_ruta(datos_insert_ruta)

    if data["resource"] == 'route' and data["event"] in ['start', 'finish']:
        datos_insert_ruta = data_beetrack.generar_data_insert_creacion_ruta(data)
        row = conn.update_route_beetrack_event(datos_insert_ruta)

        return {
            "message" : "data recibida correctamente"
            }
    
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

                print('data, YO PASE')

                # body = {
                #     "status": data["status"],
                #     "substatus": data["substatus"],
                #     "substatus_code": data["substatus_code"]
                # }

                body = conn.read_estados_paris(data["status"],data["substatus_code"])

                send_put_request(body, data["guide"])

                
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

                print('data, YO PASE')

                if data["status"] is None:
                    data["status"] = 0

                if data["substatus_code"] is None:
                    data["substatus_code"] = 0

                body = conn.read_estados_paris(data["status"],data["substatus_code"])

                # body = {
                #     "status": data["status"],
                #     "substatus": data["substatus"],
                #     "substatus_code": data["substatus_code"]
                # }

                send_put_request(body, data["guide"])

                # cliente_paris = conn.read_clientes_de_paris(dato_ruta_ty)[0]

                
                return {
                    "message" : "data recibida correctamente"
                }
                
    return {
            "message" : "data recibida correctamente"
            }

@app.post("/api/v2/beetrack/route")
async def post_route(body : Route , headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers

    return {
            "body" : body
            }



def send_put_request(payload, codigo_guia):
    # URL del endpoint
    url = f'https://cluster-staging.dispatchtrack.com/api/external/v1/dispatches/{codigo_guia}'

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
    else:
        print(f"Error en la solicitud PUT: {response.status_code}")
        print(response.text)

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

    try:

        if body["resource"] == "dispatch_guide":
            mensaje = "Recibido Modelo Creación Guia"
            data = CreacionGuia(**body)
            


        if body["resource"] == "dispatch":
            mensaje = "Recibido Modelo Actualización Guia"
            data = ActualizacionGuia(**body)

            lista_cartones = conn.get_cartones_despacho_paris(data.dispatch_id)[0]
            if lista_cartones is None:
                lista_cartones = []

            for n in range(len(data.items)):
                carton = [extra.value for extra in data.items[n].extras if extra.name == 'CARTONID'][0]
                print(carton)
                if carton not in lista_cartones :
                    
                    ingreso = construct_body_from_actualizacion_guia(data,n)
                    conn.insert_dispatch_paris(ingreso)
                    lista_cartones.append(carton)

                else:
                    print('carton ya existe', carton)


                    if data.status is None:
                        data.status = 0

                    if data.substatus_code is None:
                        data.substatus_code = 0


                    conn.update_estado_dispatch_paris(data.dispatch_id, data.status,data.substatus_code)

                    body = conn.read_estados_paris(data.status,data.substatus_code)

                    send_put_request(body, data.guide)



        # if body["resource"] == "route":
        #     mensaje = "Recibido Modelo Creación Ruta"
        #     data = CreacionRuta(**body)

        #     data = data.dict()

        #     conn.insert_creacion_ruta_paris(data)

            # Generar nombre de archivo único usando timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"datos_{timestamp}.txt"
        
        # Guardar el contenido del JSON en un archivo de texto
        with open(filename, "w") as f:
            json.dump(body, f, indent=4)

        return {
                "message": mensaje
                # "datos": data
                }
    except Exception as error:


        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"datos_por_error_400_{timestamp}.txt"
        
        # Guardar el contenido del JSON en un archivo de texto
        with open(filename, "w") as f:
            json.dump(body, f, indent=4)

        print('Error al recibir el cuerpo del mensaje de dispatch paris',error)
        raise HTTPException(status_code=400, detail="Error al recibir el cuerpo del mensaje")
    

    
@app.post("/api/v2/beetrack/dispatch_guide")
async def post_dispatch_guide(body: DistpatchGuide, headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers

    return {
            "body" : body
            }


@app.get("/api/v2/dispatch/test")
async def post_dispatch_guide(dispatch_id :int):
    
    lista_cartones = conn.get_cartones_despacho_paris(dispatch_id)

    return {
            "body" : lista_cartones[0]
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
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="la contraseña no es correcto")
    
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


# # Definir tres modelos distintos
# class ModeloA(BaseModel):
#     campo1: str
#     campo2: int

# class ModeloB(BaseModel):
#     campo3: float
#     campo4: bool

# class ModeloC(BaseModel):
#     campo5: str
#     campo6: list[int]

# # Ruta que puede recibir cualquiera de los tres modelos
# @app.post("/api/v2/reconocer/")
# async def reconocer(modelo: ModeloA | ModeloB | ModeloC):
#     if isinstance(modelo, ModeloA):
#         return {"mensaje": "Recibido Modelo A", "datos": modelo}
#     elif isinstance(modelo, ModeloB):
#         return {"mensaje": "Recibido Modelo B", "datos": modelo}
#     elif isinstance(modelo, ModeloC):
#         return {"mensaje": "Recibido Modelo C", "datos": modelo}