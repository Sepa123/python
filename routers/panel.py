from fastapi import APIRouter, status,HTTPException

##Modelos 

from database.models.panel.usuario import Usuario
from database.models.user import loginSchema

##Conexiones

from database.client import reportesConnection
from database.hela_prod import HelaConnection

from lib.password import hash_password

router = APIRouter(tags=["panel"], prefix="/api/panel")

conn = reportesConnection()

connHela = HelaConnection()

@router.post("/registrar", status_code=status.HTTP_201_CREATED)
async def registrar_usuario(usuario : Usuario):
    
    data = usuario.dict()
    mail = connHela.read_only_one(usuario.Mail)
    if mail is not None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Este correo ya esta registrado")

    connHela.insert_nuevo_usuario(data)

    return {
        "message": "Usuario registrado correctamente"
    }
    

@router.post("/password")
async def cambiar_password(body : loginSchema):
    
    row = connHela.cambiar_password(hash_password(body.password),body.mail)

    if row == 1:
        return {
            "message": "Se ha actualizado la contraseña correctamente"
        }
    else:
        return {
            "message": "No se ha actualizado la contraseña"
        }
    


@router.get("/ver/datos", status_code=status.HTTP_201_CREATED)
async def registrar_usuario(id : str, server : str):
    
    datos = connHela.mostrar_datos_usuario_hela(id)
    print(datos)

    return {
        "Telefono" : datos[0],
        "Fecha_nacimiento" : datos[1],
        "Direccion" : datos[2]
    }