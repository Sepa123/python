from fastapi import FastAPI, HTTPException, status
from database.client import UserConnection
from database.schema.user_schema import userSchema, loginSchema
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from passlib.context import CryptContext
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta

crypt = CryptContext(schemes=["bcrypt"])

## doc http://127.0.0.1:8000/docs
## doc http://127.0.0.1:8000/redoc


app = FastAPI()

conn = UserConnection()

origins = [
    "http://localhost:4200",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    conn
    return "hola"

@app.post("/insert", status_code=status.HTTP_201_CREATED)
async def insert(user_data : userSchema):
    try:
        data = user_data.dict()
        data.pop("id")
        data["password"] = crypt.hash(data["password"])
        conn.write(data)
        return { "message":"Usuario registrado correctamente" }
    except:
        print("error")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")
    


@app.get("/select",status_code=status.HTTP_202_ACCEPTED)
async def select():
    items = []
    for data in conn.read_all():
        dictionary = {}
        dictionary["id"] = data[0]
        dictionary["name"] = data[1]
        dictionary["phone"] = data[2]
        items.append(dictionary)
    
    return items

@app.post("/login", status_code=status.HTTP_202_ACCEPTED)
async def login_user(user_data:loginSchema):
    data = user_data.dict()
    user_db = conn.read_only_one(data)

    if user_db is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="El usuario no existe")
    
    if not crypt.verify(data["password"], user_db[1]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="la contraseña no es correcto")
    return "Bienvenido {}".format(data["username"])
