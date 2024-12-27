import datetime
from typing import Optional
from fastapi import FastAPI, Form, HTTPException, File, UploadFile, APIRouter
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os
from dotenv import load_dotenv
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from PIL import Image
from io import BytesIO

# from database.client import  reportesConnection
import io
# Cargar las variables de entorno desde el archivo .env
load_dotenv()


router = APIRouter(tags=["Camara"], prefix="/api/camara")

# Configuración de FastAPI
# app = FastAPI()

# Crear los parámetros de conexión usando las variables del .env
parametros_conexion = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT"),
}

# Configurar los orígenes permitidos
# origins = ["*"]

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_credentials=True,
#     allow_methods=["POST", "GET", "DELETE"],
#     allow_headers=["*"],
# )


class Evidencia(BaseModel):
    latitud: float
    longitud: float
    escaneo: str
    id_ppu: str
    ppu: str
    id_ruta: str
    ppu_png: Optional[str] = None
    imagen1_png: Optional[str] = None
    imagen2_png: Optional[str] = None
    imagen3_png: Optional[str] = None
    intentos: int
    

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

# Probar conexión a la base de datos
def get_db_connection():
    try:
        connection = psycopg2.connect(**parametros_conexion)
        return connection
    except psycopg2.Error as e:
        raise HTTPException(
            status_code=500, detail=f"Error al conectar a la base de datos: {str(e)}"
        )

@router.post("/subir-archivo/")
async def subir_archivo(
    latitud: float = Form(...),
    longitud: float = Form(...),
    escaneo: str = Form(...),
    ppu: str = Form(...),
    id_ruta: str = Form(...),
    imagen1_png: UploadFile = File(...),
    imagen2_png: UploadFile = File(...),
    imagen3_png: UploadFile = File(...),
    imagen4_png: UploadFile = File(...),
    intentos: int = Form(...)
):
    """
    Subir un archivo al servidor, registrar su ruta y guardar datos en la base de datos.
    """
    try:
        # Crear directorio basado en PPU
        if not ppu:
            raise HTTPException(status_code=400, detail="PPU es obligatorio.")
        directorio = os.path.abspath(f"archivos/{ppu}")
        os.makedirs(directorio, exist_ok=True)

        # Crear un nombre único para el archivo
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        nombre_hash = f"{ppu}_{timestamp}"
        ruta_completa = os.path.join(directorio)

        # Leer el archivo de imagen recibido
        image_bytes = await imagen1_png.read()

        # Usar PIL para abrir y procesar la imagen (si es necesario)
        image = Image.open(BytesIO(image_bytes))
        # Guardar la imagen en el servidor, por ejemplo
        # Aquí estamos usando el nombre original del archivo, pero puedes renombrarlo si es necesario
        image.save(ruta_completa+'/'+nombre_hash+'.png')



        # Leer el archivo de imagen recibido
        image_bytes2 = await imagen2_png.read()

        # Usar PIL para abrir y procesar la imagen (si es necesario)
        image = Image.open(BytesIO(image_bytes2))
        # Guardar la imagen en el servidor, por ejemplo
        # Aquí estamos usando el nombre original del archivo, pero puedes renombrarlo si es necesario
        image.save(ruta_completa+'/'+nombre_hash+'.png')


        # Leer el archivo de imagen recibido
        image_bytes3 = await imagen3_png.read()

        # Usar PIL para abrir y procesar la imagen (si es necesario)
        image2 = Image.open(BytesIO(image_bytes3))
        # Guardar la imagen en el servidor, por ejemplo
        # Aquí estamos usando el nombre original del archivo, pero puedes renombrarlo si es necesario
        image2.save(ruta_completa+'/'+nombre_hash+'.png')


        # Leer el archivo de imagen recibido
        image_bytes4 = await imagen4_png.read()

        # Usar PIL para abrir y procesar la imagen (si es necesario)
        image3 = Image.open(BytesIO(image_bytes4))
        # Guardar la imagen en el servidor, por ejemplo
        # Aquí estamos usando el nombre original del archivo, pero puedes renombrarlo si es necesario
        image3.save(ruta_completa+'/'+nombre_hash+'.png')


        

        # with open(imagen1_png, 'rb') as binary_file:
        #     binary_data = binary_file.read()

        output_image_path = os.path.join(directorio)

        print(f"Imagen guardada en: {output_image_path}")
        # Guardar el archivo
        # with open(ruta_completa, "wb") as f:
        #     contents = await imagen1_png.read()
        #     f.write(contents)

        # Registrar la ruta en la base de datos
        ruta_bd = f"archivos/{ppu}/{nombre_hash}"
        conexion = get_db_connection()
        cursor = conexion.cursor()
        imagen1_png = f"{directorio}/{nombre_hash+'.png'}" if directorio else None
        imagen2_png = f"{directorio}/{nombre_hash+'.png'}" if directorio else None
        imagen3_png = f"{directorio}/{nombre_hash+'.png'}" if directorio else None
        imagen4_png = f"{directorio}/{nombre_hash+'.png'}" if directorio else None
        consulta = """
            INSERT INTO mercadolibre.evidencia_diaria_fm
            (latitud, longitud, escaneo, ppu, id_ruta, imagen1_png, imagen2_png, imagen3_png, imagen4_png, intentos)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.execute(
            consulta, 
            (
                latitud,
                longitud,
                escaneo,
                ppu,
                id_ruta,
                imagen1_png,  # Usar la ruta del archivo 
                imagen2_png,
                imagen3_png,
                imagen4_png,
                intentos,
            )
        )
        conexion.commit()

        return {"message": "Archivo subido y registrado exitosamente.", "ruta": ruta_bd}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al subir el archivo: {str(e)}")

    finally:
        if "cursor" in locals():
            cursor.close()
        if "conexion" in locals():
            conexion.close()


@router.get("/obtener-imagen/{nombre_imagen}")
async def obtener_imagen(nombre_imagen: str):
    try:
        # Conectar a la base de datos
        conexion = get_db_connection()
        cursor = conexion.cursor()

        # Buscar la imagen por su nombre
        consulta = "SELECT contenido, tipo FROM public.imagenes_png WHERE nombre = %s"
        cursor.execute(consulta, (nombre_imagen,))
        resultado = cursor.fetchone()

        if not resultado:
            raise HTTPException(status_code=404, detail="Imagen no encontrada.")

        contenido, tipo = resultado

        # Devolver la imagen como respuesta de streaming
        return StreamingResponse(io.BytesIO(contenido), media_type=tipo)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener la imagen: {str(e)}")

    finally:
        if "cursor" in locals():
            cursor.close()
        if "conexion" in locals():
            conexion.close()

@router.get("/info")
async def Obtener_datos(Ppu: str):
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = f"select * from mercadolibre.verificar_patente_texto('{Ppu}');"
    # Ejecutar la consulta utilizando nuestra función
    datos = ejecutar_consulta(consulta)
    # Verificar si hay datos 
    if datos:
        datos_formateados = [{
                                "codigo": fila [0],
                                "glosa": fila[1],
                                "ruta_meli_result": fila[2],
                                "patente_result": fila[3]

                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)