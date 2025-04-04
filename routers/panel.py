from typing import Optional
from fastapi import APIRouter, File, Form, UploadFile, status,HTTPException
import os
import glob
from fastapi.responses import FileResponse
import psycopg2
##Modelos 

from database.models.operaciones.centro_operacion import UpdateCentroOperacion
from database.models.panel.usuario import Usuario, CambiarPassword, DatosUsuario
from database.models.user import loginSchema

##Conexiones

from database.client import reportesConnection
from database.hela_prod import HelaConnection

from database.schema.operaciones.centro_operacion import centro_operacion_usuario_schema, co_lista_coordinador_schema
from database.schema.operaciones.supervisores import datos_supervisores_schema
from lib.password import hash_password
import bcrypt  # Importa bcrypt para encriptar contraseñas

### rodrigo

from dotenv import load_dotenv
from pydantic import BaseModel

from PIL import Image
from io import BytesIO
import datetime


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
    
    # if server == 'portal':
    #     return{
    #     "Telefono" :None,
    #     "Fecha_nacimiento" : None,
    #     "Direccion" : None,
    #     "Imagen_perfil" : None,
    #     "Rol" : None
    # }

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

    # if ids_user == 'portal':
    #     raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No puede subir imagenes de perfil")

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

    # if body.Server == 'portal':
    #     return {
    #         "message": "No se han actualizado los datos"
    #     }
    
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
    

@router.get("/ver/supervisores")
async def get_datos_supervisores_hela():
     # Consulta SQL para obtener datos (por ejemplo)
    results = conn.buscar_datos_supervisores_hela()
    return datos_supervisores_schema(results)






#### Panel Rodrigo

load_dotenv()
# Crear los parámetros de conexión usando las variables del .env
parametros_conexion = {
     "host": os.getenv("DB_HOST"),
     "database": os.getenv("DB_NAME"),
     "user": os.getenv("DB_USER"),
     "password": os.getenv("DB_PASSWORD"),
     "port": os.getenv("DB_PORT")
 }

def ejecutar_consulta(sql):
     try:
         conexion = psycopg2.connect(**parametros_conexion)
         cursor = conexion.cursor()
         cursor.execute(sql)
         filas = cursor.fetchall()
         cursor.close()
         conexion.close()
         return filas
     except Exception as e:
         raise HTTPException(status_code=500, detail=str(e))
     


class Usuario(BaseModel):
    nombre: str
    mail: str
    password: str
    activate : bool
    rol_id:str
    telefono: str
    fecha_nacimiento: str
    direccion: str
    area_id: str
    cargo: str
    id_supervisor: Optional[int] = None

class UsuarioUpdate(BaseModel):
    nombre: Optional[str] = None
    mail: Optional[str] = None
    password: Optional[str] = None
    activate: Optional[bool] = None
    rol_id: Optional[int] = None
    id_supervisor: Optional[int] = None
    telefono: Optional[str] = None
    fecha_nacimiento: Optional[str] = None  # Considerar usar datetime
    direccion: Optional[str] = None
    area_id: Optional[int] = None
    cargo: Optional[str] = None

class Bitacora(BaseModel):
    id_user: str
    ids_user: Optional[str] = None
    origen: str
    dato_actual: Optional[str] = None
    dato_resultado: Optional[str] = None
    tabla_impactada: str

def ejecutar_consulta(sql):
    try:
        conexion = psycopg2.connect(**parametros_conexion)
        cursor = conexion.cursor()
        cursor.execute(sql)
        filas = cursor.fetchall()
        cursor.close()
        conexion.close()
        return filas
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@router.post("/Agregar/Bitacora/")
async def agregar_bitacora(body: Bitacora):
    """
    Endpoint para insertar un registro en la tabla areati.bitacora_general.
    """
    try:
        conexion = psycopg2.connect(**parametros_conexion)
        cursor = conexion.cursor()
        consulta = """
            INSERT INTO areati.bitacora_general
                (id_user, ids_user, origen, dato_actual, dato_resultado, tabla_impactada)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        # Parámetros en el mismo orden que los placeholders
        parametros = (
            body.id_user, body.ids_user, body.origen, 
            body.dato_actual, body.dato_resultado, body.tabla_impactada
        )
        cursor.execute(consulta, parametros)
        conexion.commit()
        cursor.close()
        conexion.close()
        return {"message": "Registro agregado a la bitácora correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al insertar en la bitácora: {str(e)}")
    
@router.get("/cargarUsuarios/GestionyMantencion")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = "select * from taskmaster.retorna_listado_usuarios();"
    # Ejecutar la consulta utilizando nuestra función
    datos = ejecutar_consulta(consulta)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "usuario_id" : fila[0],
                                "usuario_nombre": fila[1],
                                "usuario_mail": fila [2],
                                "usuario_telefono": fila[3],
                                "area_id" : fila[4],
                                "area_nombre" : fila[5],
                                "rol_id" : fila[6],
                                "rol_nombre": fila[7],
                                "imagen_perfil": fila[8],
                                "activate": fila[9],
                                "area_icono": fila[10],
                                "area_color": fila[11],                    
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")


@router.get("/Rol/")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = "select * from hela.rol"
    # Ejecutar la consulta utilizando nuestra función
    datos = ejecutar_consulta(consulta)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "nombre": fila[1],
                                                
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")


@router.get("/area/")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = "select * from taskmaster.areas"
    # Ejecutar la consulta utilizando nuestra función
    datos = ejecutar_consulta(consulta)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "nombre": fila[1],
                                "icono": fila[2],
                                "color": fila[3],
                                                
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/usuarios/")
async def User_data(id: str):
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = f"select * from hela.usuarios u where id = {id}"
    # Ejecutar la consulta utilizando nuestra función
    datos = ejecutar_consulta(consulta)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "nombre": fila[1],
                                "mail": fila[2],
                                "activate": fila[4],
                                "rol_id": fila[5],
                                "telefono": fila[6],
                                "fecha_nacimiento": fila[7],
                                "direccion": fila[8],
                                "imagen_perfil": fila[9],
                                "area_id": fila[10],
                                "cargo": fila[11],
                                "id_supervisor": fila[12],
                                                
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    
@router.get("/Supervisor/")
async def supervisor_data():
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = """select * from hela.usuarios u where activate = true;"""
    # Ejecutar la consulta utilizando nuestra función
    datos = ejecutar_consulta(consulta)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "nombre": fila[1],
                                                
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    
@router.post("/Agregar/Usuario/")
async def Agregar_newUserHela(body: Usuario):
    try:
        conexion = psycopg2.connect(**parametros_conexion)
        cursor = conexion.cursor()
        consulta = """
            INSERT INTO hela.usuarios 
                (nombre, mail, password, activate, rol_id, telefono, 
                 fecha_nacimiento, direccion, id_area, cargo, id_supervisor) 
            VALUES 
                (%s, %s, upper(MD5(%s)), %s, %s, %s, %s, %s, %s, %s, %s);
        """
        # Parámetros en una tupla en el mismo orden que los placeholders
        parametros = (
            body.nombre, body.mail, body.password, body.activate, 
            body.rol_id, body.telefono, body.fecha_nacimiento, 
            body.direccion, body.area_id, body.cargo, body.id_supervisor
        )
        cursor.execute(consulta, parametros)
        conexion.commit()
        cursor.close()
        conexion.close()
        return {"message": "Usuario agregado correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/subir-archivo/fotoPerfil/")
async def subir_archivo(
    id_user: str = Form(...),
    imagen1_png: UploadFile = File(...)
):
    """
    Subir un archivo al servidor, registrar su ruta y guardar datos en la base de datos.
    """
    try:
        # Validar que el ID del usuario no esté vacío
        if not id_user:
            raise HTTPException(status_code=400, detail="Id del usuario es obligatorio.")
        
        # Crear directorio para guardar la imagen
        directorio = os.path.abspath("image/foto_perfil/")
        os.makedirs(directorio, exist_ok=True)
        
        # Crear un nombre único para el archivo
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        nombre_hash = f"{id_user}_{timestamp}"
        extension = os.path.splitext(imagen1_png.filename)[1].lower()  # Obtener la extensión del archivo
        ruta_completa = os.path.join(directorio, f"{nombre_hash}{extension}")
        
        # Leer el archivo de imagen recibido
        image_bytes = await imagen1_png.read()
        
        # Verificar si el archivo es un GIF
        if extension == ".gif":
            # Guardar el archivo directamente sin procesarlo
            with open(ruta_completa, "wb") as f:
                f.write(image_bytes)
        else:
            # Usar PIL para abrir y procesar la imagen
            image = Image.open(BytesIO(image_bytes))
            image.save(ruta_completa)  # Guardar la imagen en el servidor
        
        print(f"Imagen guardada en: {ruta_completa}")
        
        # Registrar la ruta en la base de datos
        ruta_bd = f"{imagen1_png.filename}"
        conexion = psycopg2.connect(**parametros_conexion)  # Crear conexión a la base de datos
        cursor = conexion.cursor()
        
        consulta = """
            UPDATE hela.usuarios
            SET imagen_perfil = %s
            WHERE id = %s;
        """
        cursor.execute(consulta, (ruta_bd, id_user))
        conexion.commit()
        
        return {"message": "Archivo subido y registrado exitosamente.", "ruta": ruta_bd}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al subir el archivo: {str(e)}")
    
    finally:
        # Cerrar cursor y conexión si existen
        if "cursor" in locals():
            cursor.close()
        if "conexion" in locals():
            conexion.close()

@router.patch("/Actualizar/Usuario/{usuario_id}")
async def actualizar_usuarioHela(usuario_id: int, body: UsuarioUpdate):
    try:
        conexion = psycopg2.connect(**parametros_conexion)
        cursor = conexion.cursor()
        # Construir dinámicamente la consulta UPDATE
        update_fields = []
        params = []
        
        for field, value in body.dict(exclude_unset=True).items():
            # Mapear nombres de campos si es necesario (ej: area_id -> id_area)
            db_field = field
            if field == "area_id":
                db_field = "id_area"
            
            # Encriptar el password si está presente
            if field == "password" and value:
                # Usar MD5 en la consulta SQL
                update_fields.append(f"{db_field} = upper(MD5(%s))")
                params.append(value)  # Agregar el valor del password a params
            else:
                update_fields.append(f"{db_field} = %s")
                params.append(value)
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")
        
        # Agregar el WHERE al final de los parámetros
        params.append(usuario_id)
        
        consulta = f"""
            UPDATE hela.usuarios
            SET {", ".join(update_fields)}
            WHERE id = %s
        """
        cursor.execute(consulta, tuple(params))
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        conexion.commit()
        
        return {"message": "Usuario actualizado correctamente"}
        
    except Exception as e:
        conexion.rollback()
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(e)}")
    finally:
        cursor.close()
        conexion.close()