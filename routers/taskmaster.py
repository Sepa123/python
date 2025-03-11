from io import BytesIO
import os
from fastapi import APIRouter, File, UploadFile, status,HTTPException
from PIL import Image
## modelos y schemas

from database.client import reportesConnection


from database.models.taskmaster.activos import Activo

router = APIRouter(tags=["task_master"], prefix="/api/task")

conn = reportesConnection()


    

@router.post("/activos")
async def cambio_razon(body: Activo):
    try:
        # conn.cambiar_razon_social_vehiculo(razon_id,id)
        data = body.dict()
        conn.insert_activos_taskmaster(data)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.get("/datos/gestor-activos")
async def datos_vehiculos():
    datos = conn.datos_seleccion_taskmasters()
    resultado_dict = {titulo : cant for titulo, cant in datos}

    return resultado_dict


@router.post("/subir/fotos")
async def cambio_razon(imagen1_png: UploadFile = File(...),imagen2_png: UploadFile = File(...),imagen3_png: UploadFile = File(...)):
    try:
        # conn.cambiar_razon_social_vehiculo(razon_id,id)

        directorio  = os.path.abspath(f"image/foto_perfil")

        nombre_imagen = "f'{id_user}_'+file.filename"

        ruta = os.path.join(directorio,nombre_imagen)

        # Leer el archivo de imagen recibido
        image_bytes = await imagen1_png.read()

        # Usar PIL para abrir y procesar la imagen (si es necesario)
        image = Image.open(BytesIO(image_bytes))
        # Guardar la imagen en el servidor, por ejemplo
        # Aquí estamos usando el nombre original del archivo, pero puedes renombrarlo si es necesario
        image.save("ruta_completa"+'/'+"nombre_hash"+'_1.png')
        
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

