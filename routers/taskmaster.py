from fastapi import APIRouter, status,HTTPException

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