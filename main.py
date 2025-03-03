from fastapi import FastAPI, HTTPException, Request, status, Depends
from database.client import UserConnection 
from database.hela_prod import HelaConnection
from database.schema.user_schema import users_schema, user_schema
from database.models.user import userSchema, loginSchema ,User
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from datetime import datetime, timedelta
from lib.password import verify_password, hash_password
from database.models.token import TokenPayload
from routers import finanzas, inventario,areati, carga,panel,electrolux, transyanez, reportes_cargas, pedidos, productos, rutas, recepcion, comunas, clientes, toc , rsv, beetrack, easy,logistica_inversa,seguridad, meli , operaciones, venta_traspaso, CamaraPpu,taskmaster
from database.schema.roles_list import roles_list_schema
import time,re


# import os
# import multiprocessing

## documentacion api

# from fastapi.openapi.docs import get_swagger_ui_html
# from fastapi.openapi.utils import get_openapi
# from starlette.responses import HTMLResponse, JSONResponse

SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 100

crypt = CryptContext(schemes=["bcrypt"])

## doc http://127.0.0.1:8000/docs
## doc http://127.0.0.1:8000/redoc

oauth2 = OAuth2PasswordBearer(tokenUrl="/login")

app = FastAPI(docs_url="/api/docs", redoc_url="/api/redoc")

app.include_router(areati.router)
app.include_router(transyanez.router)
app.include_router(reportes_cargas.router)
app.include_router(pedidos.router)
app.include_router(productos.router)
app.include_router(rutas.router)
app.include_router(recepcion.router)
app.include_router(panel.router)
app.include_router(comunas.router)
app.include_router(clientes.router)
app.include_router(carga.router)
app.include_router(toc.router)
app.include_router(rsv.router)
app.include_router(beetrack.router)
app.include_router(electrolux.router)
app.include_router(easy.router)
app.include_router(logistica_inversa.router)
app.include_router(inventario.router)
app.include_router(seguridad.router)
app.include_router(meli.router)
app.include_router(operaciones.router)
app.include_router(finanzas.router)
app.include_router(venta_traspaso.router)
app.include_router(CamaraPpu.router)
app.include_router(taskmaster.router)

conn = UserConnection()
hela_conn = HelaConnection()

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
    "https://melifm.transyanez.cl"

]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(GZipMiddleware, minimum_size=10000)

@app.get("/api")
def root():
    return "hola"

# @app.get("/api/cpu")
# async def cpu():
#  # Obtener la cantidad de núcleos de la CPU
#     num_nucleos = multiprocessing.cpu_count()

#     # Obtener información adicional sobre la CPU
#     info_cpu = os.popen("lscpu").read()

#     return {
#         "nucleos": num_nucleos,
#         "info_cpu" : info_cpu
#     }

# @app.get("/api/docs", response_class=HTMLResponse)
# async def custom_swagger_ui_html():
#     openapi_url = app.url_path_for("openapi")
#     return get_swagger_ui_html(openapi_url, title=app.title + " - Swagger UI")


# @app.get("/openapi.json", tags=["openapi"])
# async def custom_openapi():
#     return JSONResponse(get_openapi(title=app.title, version=app.version))


# @app.post("/insert", status_code=status.HTTP_201_CREATED)
# async def insert(user_data : userSchema):
#     try:
#         data = user_data.dict()
#         data.pop("id")
#         data["password"] = crypt.hash(data["password"])
#         conn.write(data)
#         return { "message":"Usuario registrado correctamente" }
#     except:
#         print("error")
#         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")

@app.get("/api/select",status_code=status.HTTP_202_ACCEPTED)
async def select():
    users_db = conn.read_all()
    if not users_db:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")
    
    return users_schema(users_db)


@app.post("/api/login", status_code=status.HTTP_202_ACCEPTED)
def login_user(user_data:loginSchema):
    data = user_data.dict()
    user_db = hela_conn.read_only_one(user_data.mail.lower())
    server = "hela"

    if user_db is None:
        user_db = conn.read_only_one(data)
        server = "portal"
        if user_db is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="El usuario no existe")
    
    if not verify_password(data["password"],user_db[3]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="la contraseña no es correcto")
    

    if not user_db[4]:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="El usuario esta inactivo")
    
    # return user_db
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
        "access_token": jwt.encode(access_token, SECRET_KEY,algorithm=ALGORITHM),
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
        username = jwt.decode(token, key=SECRET_KEY, algorithms=[ALGORITHM])
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

@app.get("/api/user")
def me (user:TokenPayload = Depends(current_user)):

    return user

@app.get("/api/roles")
async def me ():  
    results = conn.read_roles()
    return roles_list_schema(results)

def valida_rut(rut_completo):
    rut_completo = rut_completo.replace("‐","-");
    if not re.match(r'^[0-9]+[-|‐]{1}[0-9kK]{1}$', rut_completo, re.IGNORECASE):
        return False

    rut, digv = rut_completo.split('-')

    if digv == 'K':
        digv = 'k'
    return calcular_dv(rut) == digv

def calcular_dv(T):
    M = 0
    S = 1
    while T:
        S = (S + int(T) % 10 * (9 - M % 6)) % 11
        M += 1
        T = int(T) // 10
    return str(S - 1) if S else 'k'


@app.get("/api/validar/rut")
async def validarRut (rut : str): 
    rut = rut.replace('.','')
    return {
        'message' : 'Valido' if valida_rut(rut.strip()) else 'inválido'
    }



@app.get("/api/test/comando")
async def test_comandos():  

    return {
        "message" : f"Resp "
        }


@app.get("/api/client-info")
async def client_info(request: Request):
    client_ip = request.client.host  # Dirección IP del cliente
    user_agent = request.headers.get("User-Agent")  # Agente de usuario del cliente
    return {"client_ip": client_ip, "user_agent": user_agent}