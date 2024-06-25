from fastapi import APIRouter, File, UploadFile, status,HTTPException
import os

from fastapi.responses import FileResponse
##Modelos 

from database.models.panel.usuario import Usuario, CambiarPassword
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
        "Direccion" : datos[2],
        "Imagen_perfil" : datos[3],
        "Rol" : datos[4]
    }



@router.post("/nueva/password")
async def cambiar_password_nueva(body : CambiarPassword):
    
    row = connHela.cambiar_password_nueva(hash_password(body.Password_antigua),body.Mail,hash_password(body.Password_nueva))

    if row == 1:
        return {
            "message": "Se ha actualizado la contraseña correctamente"
        }
    else:
        return {
            "message": "No se ha actualizado la contraseña"
        }
    
    
@router.post("/subir-imagen", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo(id_user : str, ids_user : str, file: UploadFile = File(...)):

    directorio  = os.path.abspath(f"image/foto_perfil")

    nombre_imagen = f'{id_user}_'+file.filename

    ruta = os.path.join(directorio,nombre_imagen)

    with open(ruta, "wb") as f:
        contents = await file.read()
        # print("pase por aqui")
        f.write(contents)
    
    connHela.actualizar_imagen_perfil(id_user,nombre_imagen)

    return {
       "filename": nombre_imagen
    }
    
@router.get("/foto-perfil")
def download_file(name_file: str):
 
    return FileResponse(f"image/foto_perfil/{name_file}")



# Ruta del directorio donde se almacenan las imágenes
IMAGE_DIR = "image/foto_perfil"

@router.get("/image/foto_perfil/{image_name}")
async def get_image(image_name: str):
    image_path = os.path.join(IMAGE_DIR, image_name)
    if os.path.exists(image_path):
        return FileResponse(image_path)
    else:
        raise HTTPException(status_code=404, detail="Image not found")

    