from fastapi import APIRouter, File, UploadFile, status,HTTPException
import os
import glob
from fastapi.responses import FileResponse
##Modelos 

from database.models.operaciones.centro_operacion import UpdateCentroOperacion
from database.models.panel.usuario import Usuario, CambiarPassword, DatosUsuario
from database.models.user import loginSchema

##Conexiones

from database.client import reportesConnection
from database.hela_prod import HelaConnection

from database.schema.operaciones.centro_operacion import centro_operacion_usuario_schema, co_lista_coordinador_schema
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
    
    if server == 'portal':
        return{
        "Telefono" :None,
        "Fecha_nacimiento" : None,
        "Direccion" : None,
        "Imagen_perfil" : None,
        "Rol" : None
    }

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
    

# Ruta del directorio donde se almacenan las imágenes
IMAGE_DIR = "image/foto_perfil"
    
@router.post("/subir-imagen", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo(id_user : str, ids_user : str, file: UploadFile = File(...)):

    if ids_user == 'portal':
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No puede subir imagenes de perfil")

    archivos = glob.glob(os.path.join(IMAGE_DIR, f'{id_user}_*'))

    for archivo in archivos:
        os.remove(archivo)
        print(f'Eliminado: {archivo}')

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





@router.get("/image/foto_perfil/{image_name}")
async def get_image(image_name: str):
    image_path = os.path.join(IMAGE_DIR, image_name)
    if os.path.exists(image_path):
        return FileResponse(image_path)
    else:
        raise HTTPException(status_code=404, detail="Image not found")


@router.put("/actualizar/datos/usuario")
async def actualizar_datos_usuario(body : DatosUsuario):

    if body.Server == 'portal':
        return {
            "message": "No se han actualizado los datos"
        }
    
    data = body.dict()
    
    row = connHela.actualizar_datos_usuario(data)

    if row == 1:
        return {
            "message": "Se han actualizado los datos correctamente"
        }
    else:
        return {
            "message": "No se han actualizado los datos"
        }
    


@router.get("/centro_operacion/usuario")
async def get_image(id: str, server : str):
    ids_usuario = server+'-'+id
    
    data = conn.buscar_centro_operacion_usuario(ids_usuario)

    return centro_operacion_usuario_schema(data)



@router.get("/centro_operacion/lista")
async def get_co_op_lista_coordinadores(id: str):
### Centros de operacion, EXCLUYENDO id de usuario entregada
    results = conn.buscar_centro_operacion_lista(id)

    return co_lista_coordinador_schema(results)

@router.post("/asignar/coordinador/co")
async def asignar_coordinador_a_co(body : UpdateCentroOperacion):
    
    data = body.dict()
    
    row = conn.asignar_coordinador_centro_operacion(data)

    if row == 1:
        return {
            "message": "Se han actualizado los datos correctamente"
        }
    else:
        return {
            "message": "No se han actualizado los datos"
        }
    


@router.post("/eliminar/coordinador/co")
async def eliminar_coordinador_a_co(body : UpdateCentroOperacion):
    
    data = body.dict()
    
    row = conn.eliminar_coordinador_centro_operacion(data)

    if row == 1:
        return {
            "message": "Se han actualizado los datos correctamente"
        }
    else:
        return {
            "message": "No se han actualizado los datos"
        }