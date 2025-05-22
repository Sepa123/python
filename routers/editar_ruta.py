import datetime
from fastapi import APIRouter, FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Form, HTTPException, File, UploadFile
import psycopg2
import uvicorn
import os
from typing import Optional
from dotenv import load_dotenv
from PIL import Image
from io import BytesIO
import bcrypt  # Importa bcrypt para encriptar contraseñas
# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# app = FastAPI()

router = APIRouter(tags=["Editar_Ruta"], prefix="/api/editar_ruta")
# Crear los parámetros de conexión usando las variables del .env
parametros_conexion = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT")
}

# Configurar los orígenes permitidos (permite todas las solicitudes por ahora)
origins = ["*"]

# Agregar middleware CORS a la aplicación
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

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
    carga_manual: Optional[bool] = None
    id_operacion: Optional[int] = None
    id_centro_op: Optional[int] = None
    id_seguimiento: Optional[int] = None

class Cliente(BaseModel):
    id_usuario: int
    ids_usuario: str
    nombre: str
    rut: str
    direccion: str
    ciudad: int
    region: int
    telefono: str
    correo: str
    representante: str
    activo: bool
    esquema_destino: str
    tabla_destino: str
    carga_manual: bool
    id_operacion: int
    id_centro_op: int
    id_seguimiento: Optional[int] = None

class ClienteUpdate(BaseModel):
    id_usuario: Optional[int] = None
    ids_usuario: Optional[str] = None
    nombre: Optional[str] = None
    rut: Optional[str] = None
    direccion: Optional[str] = None
    ciudad: Optional[int] = None
    region: Optional[int] = None
    telefono: Optional[str] = None
    correo: Optional[str] = None
    representante: Optional[str] = None
    activo: Optional[bool] = None
    esquema_destino: Optional[str] = None
    tabla_destino: Optional[str] = None
    carga_manual: Optional[bool] = None
    id_operacion: Optional[int] = None
    id_centro_op: Optional[int] = None
    id_seguimiento: Optional[int] = None
    

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
    

@router.get("/InfoRuta")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = "select * from rutas.clientes c "
    # Ejecutar la consulta utilizando nuestra función
    datos = ejecutar_consulta(consulta)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{  
                                "id" : fila[0],
                                "nombre" : fila[5],
                                "rut": fila[6],
                                "direccion": fila [7],
                                "ciudad": fila[8],
                                "region" : fila[9],
                                "telefono" : fila[10],
                                "correo" : fila[11],
                                "representante": fila[12],
                                "activo": fila[13],
                                "logo_img" : fila[14],
                                "carga_manual": fila[20],
                                "id_operacion": fila[21],
                                "id_centro_op": fila[22],
                                "id_seguimiento": fila[23],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

    
@router.get("/clientes/")
async def User_data(id: str):
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = f"select * from rutas.clientes c where id = '{id}' "
    # Ejecutar la consulta utilizando nuestra función
    datos = ejecutar_consulta(consulta)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "nombre" : fila[5],
                                "rut": fila[6],
                                "direccion": fila [7],
                                "ciudad": fila[8],
                                "region" : fila[9],
                                "telefono" : fila[10],
                                "correo" : fila[11],
                                "representante": fila[12],
                                "activo": fila[13],
                                "logo_img" : fila[14],
                                "carga_manual": fila[20],
                                "id_operacion": fila[21],
                                "id_centro_op": fila[22],
                                "id_seguimiento": fila[23],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")


@router.get("/regiones/")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = "select * from public.op_regiones or2 "
    # Ejecutar la consulta utilizando nuestra función
    datos = ejecutar_consulta(consulta)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id_region" : fila[0],
                                "region_num": fila[1],
                                "region_name": fila[2],                                               
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")


@router.get("/Comunas/")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = "select * from public.op_comunas oc "
    # Ejecutar la consulta utilizando nuestra función
    datos = ejecutar_consulta(consulta)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id_comuna" : fila[0],
                                "Comuna_name": fila[1],
                                "id_region": fila[2],
                                                
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/Op/")
async def Obtener_Op():
      # Consulta SQL para obtener datos (por ejemplo)
     consulta = """select * from operacion.modalidad_operacion mo """
     # Ejecutar la consulta utilizando nuestra función
     datos = ejecutar_consulta(consulta)
     # Verificar si hay datos
     if datos:
         datos_formateados = [{
                                 "id" : fila[0],
                                 "centro": fila[4],
                                                 
                             } 
                             for fila in datos]
         return datos_formateados
     else:
         raise HTTPException(status_code=404, detail="No se encontraron datos")
     
@router.get("/Cop/")
async def Obtener_Cop():
      # Consulta SQL para obtener datos (por ejemplo)
     consulta = """select * from operacion.centro_operacion co  """
     # Ejecutar la consulta utilizando nuestra función
     datos = ejecutar_consulta(consulta)
     # Verificar si hay datos
     if datos:
         datos_formateados = [{
                                 "id" : fila[0],
                                 "id_op": fila[4],
                                 "centro": fila[5],
                                                 
                             } 
                             for fila in datos]
         return datos_formateados
     else:
         raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/Sc/")
async def obtener_modo_seguimiento_cliente():
    """
    Obtiene los modos de seguimiento de clientes desde rutas.modo_seguimiento_cliente.
    """
    try:
        consulta = """select * from rutas.modo_seguimiento_cliente"""
        datos = ejecutar_consulta(consulta)
        if datos:
            # Puedes ajustar los índices según las columnas que devuelve la función
            resultados = [
                {
                    "id": fila[0],
                    "glosa": fila[1],
                    "icono": fila[2],
                }
                for fila in datos
            ]
            return resultados
        else:
            raise HTTPException(status_code=404, detail="No se encontraron datos")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener modos de seguimiento: {str(e)}")


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
        ruta_completa = os.path.join(directorio, imagen1_png.filename)
        
        # Leer el archivo de imagen recibido
        image_bytes = await imagen1_png.read()
        
        # Usar PIL para abrir y procesar la imagen
        image = Image.open(BytesIO(image_bytes))
        image.save(ruta_completa)  # Guardar la imagen en el servidor
        
        print(f"Imagen guardada en: {ruta_completa}")
        
        # Registrar la ruta en la base de datos
        ruta_bd = f"{imagen1_png.filename}"
        conexion = psycopg2.connect(**parametros_conexion)  # Crear conexión a la base de datos
        cursor = conexion.cursor()
        
        consulta = """
            UPDATE rutas.clientes
            SET logo_img = %s
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

@router.patch("/Actualizar/Cliente/{cliente_id}")
async def actualizar_cliente(cliente_id: int, body: ClienteUpdate):
    """
    Endpoint para actualizar un cliente en la tabla rutas.clientes.
    """
    try:
        conexion = psycopg2.connect(**parametros_conexion)
        cursor = conexion.cursor()
        # Construir dinámicamente la consulta UPDATE
        update_fields = []
        params = []
        
        for field, value in body.dict(exclude_unset=True).items():
            # Mapear nombres de campos si es necesario
            db_field = field  # En este caso, no hay mapeo adicional
            update_fields.append(f"{db_field} = %s")
            params.append(value)
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="No se proporcionaron campos para actualizar")
        
        # Agregar el WHERE al final de los parámetros
        params.append(cliente_id)
        
        consulta = f"""
            UPDATE rutas.clientes
            SET {", ".join(update_fields)}
            WHERE id = %s
        """
        cursor.execute(consulta, tuple(params))
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Cliente no encontrado")
        conexion.commit()
        
        return {"message": "Cliente actualizado correctamente"}
        
    except Exception as e:
        conexion.rollback()
        raise HTTPException(status_code=500, detail=f"Error de base de datos: {str(e)}")
    finally:
        cursor.close()
        conexion.close()



@router.post("/Agregar/Cliente/")
async def agregar_cliente(body: Cliente):
     """
     Endpoint para insertar un registro en la tabla rutas.clientes.
     """
     try:
         conexion = psycopg2.connect(**parametros_conexion)
         cursor = conexion.cursor()
         consulta = """
             INSERT INTO rutas.clientes
                 (id_usuario, ids_usuario, nombre, rut, direccion, ciudad, region, telefono, correo, representante, activo, esquema_destino, tabla_destino, carga_manual, id_operacion, id_centro_op)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
         """
         # Parámetros en el mismo orden que los placeholders
         parametros = (
             body.id_usuario, body.ids_usuario, body.nombre, body.rut, body.direccion,
             body.ciudad, body.region, body.telefono, body.correo, body.representante,
             body.activo, body.esquema_destino, body.tabla_destino, body.carga_manual,
             body.id_operacion, body.id_centro_op
         )
         cursor.execute(consulta, parametros)
         conexion.commit()
         cursor.close()
         conexion.close()
         return {"message": "Cliente agregado correctamente"}
     except Exception as e:
         raise HTTPException(status_code=500, detail=f"Error al insertar cliente: {str(e)}")
# if __name__ == "__main__":
#  uvicorn.run(app, host="0.0.0.0", port=8000)