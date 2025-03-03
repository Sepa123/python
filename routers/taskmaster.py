from fastapi import APIRouter, status,HTTPException

## modelos y schemas

from database.client import reportesConnection


from database.models.taskmaster.activos import Activo

router = APIRouter(tags=["task_master"], prefix="/api/task")

conn = reportesConnection()

@router.get("/InfoPpu")
async def Obtener_datos_PPU():
     # Consulta SQL para obtener datos (por ejemplo)
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_datos_PPU_ventas_traspaso()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{  
                                "id": fila[0],
                                "ppu" : fila[1],
                                "tipo_vehiculo": fila [2],
                                "tipo_vehiculo_descripcion":fila[3],
                                "modelo": fila[4],
                                "id_razon_social":fila[5],
                                "razon_social":fila[6],
                                "rut":fila[7],
                                "giro":fila[8]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    

@router.get("/InfoEmp")
async def Obtener_datos_Emp():
     # Consulta SQL para obtener datos (por ejemplo)
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_datos_Emp_ventas_traspaso()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "tipo_razon": fila[1],
                                "razon_social": fila[2],
                                "giro": fila[3],
                                "rut":fila[4]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    

@router.post("/activos")
async def cambio_razon(body: Activo):
    try:
        # conn.cambiar_razon_social_vehiculo(razon_id,id)
        data = body.dict()
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))



@router.post("/bitacora")
async def ingresoBitacora(id_ppu: int, id_razon_Antigua: int, id_razon_nueva:int, observacion:str):
    try:
        conn.ingreso_bitacora_cambio_razon_ppu(id_ppu,id_razon_Antigua,id_razon_nueva,observacion)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))