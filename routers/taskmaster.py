import datetime
from io import BytesIO
import os
from typing import Optional
from fastapi import APIRouter, File, Form, UploadFile, status,HTTPException
from PIL import Image
## modelos y schemas

from database.client import reportesConnection


from database.models.taskmaster.activos import Activo, ImagenesActivo, ActualizaEstadoActivos

router = APIRouter(tags=["task_master"], prefix="/api/task")

conn = reportesConnection()


@router.get("/lista/activos")
async def get_lista_activos():
    try:
        return conn.get_activos_taskmaster()[0]
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))    

@router.post("/activos")
async def registrar_activos(body: Activo):
    try:
        id_activo = conn.get_max_id_activos()[0]
        data = body.dict()
        conn.insert_activos_taskmaster(data)
        

        return {"message": "Datos Ingresados Correctamente", 
                "id_activo": id_activo}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.get("/datos/gestor-activos")
async def datos_vehiculos():
    datos = conn.datos_seleccion_taskmasters()
    resultado_dict = {titulo : cant for titulo, cant in datos}

    return resultado_dict

@router.put("/estado/activo", status_code=status.HTTP_202_ACCEPTED)
async def actualizar_estado_activo(body: ActualizaEstadoActivos):
    try:

        rows = conn.actualizar_estados_activos(body.id)

        return { "message": f"Activo actualizado correctamente." }
    except:
          print("error con verificar electrolux")
          if rows == 0:
               raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El  activo no se pudo verificar")
          
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la verificación")


@router.post("/subir/fotos")
async def cambio_razon(id_activo: int = Form(...), imagen1_png: Optional[UploadFile] = File(None),imagen2_png: Optional[UploadFile] = File(None),imagen3_png: Optional[UploadFile] = File(None),):
    try:
        # conn.cambiar_razon_social_vehiculo(razon_id,id)

        directorio = os.path.abspath(f"archivos/taskmaster/{id_activo}/fotos")
        os.makedirs(directorio, exist_ok=True)

        # Crear un nombre único para el archivo
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        nombre_hash = f"{id_activo}_{timestamp}"
        ruta_completa = os.path.join(directorio)


        # Función para procesar y guardar una imagen si fue subida
        async def guardar_imagen(imagen: UploadFile, nombre_default: str):
            if imagen and imagen.filename != "undefined":
                image_bytes = await imagen.read()
                image = Image.open(BytesIO(image_bytes))
                ruta_imagen = os.path.join(ruta_completa, imagen.filename)
                image.save(ruta_imagen)
                return directorio + '/' + imagen.filename
            return None

        ruta_imagen1_png = await guardar_imagen(imagen1_png, "imagen1.png")
        ruta_imagen2_png = await guardar_imagen(imagen2_png, "imagen2.png")
        ruta_imagen3_png = await guardar_imagen(imagen3_png, "imagen3.png")


        print('ruta_imagen1_png',ruta_imagen1_png)
        print('ruta_imagen2_png',ruta_imagen2_png)  
        print('ruta_imagen3_png',ruta_imagen3_png)

        conn.agregar_imagenes_activo(ruta_imagen1_png,ruta_imagen2_png,ruta_imagen3_png,id_activo)

        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))



@router.post("/subir/archivo", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo(id : str, file: UploadFile = File(...)):

    directorio = os.path.abspath(f"archivos/taskmaster/{id}/archivo_adjunto")
    os.makedirs(directorio, exist_ok=True)


    ruta = os.path.join(directorio,file.filename)

    with open(ruta, "wb") as f:
        contents = await file.read()
        f.write(contents)


    conn.agregar_archivo_adjunto_activo(f'archivos/taskmaster/{id}/archivo_adjunto/{file.filename}',id)
