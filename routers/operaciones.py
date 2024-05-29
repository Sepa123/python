from fastapi import APIRouter,status,UploadFile, File,HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from database.client import reportesConnection
from database.models.operaciones.modalidad_operacion import RazonSocial, updateApp
from database.models.operaciones.centro_operacion import CentroOperacion
from database.schema.operaciones.centro_operacion import centro_operacion_schema, centro_operacion_asignado_schema
import psycopg2
import uvicorn

router = APIRouter(tags=["modalidadOperacion"],prefix="/api/operacion")
parametros_conexion = {

}

conn = reportesConnection()

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
    

@router.post("/agregar/razonSocial")
async def Agregar_RazonSocial(body: RazonSocial):
    try:
        data = body.dict()
        conn.agregar_razon_social(data)

        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/modalidad")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.buscar_modalidad_operacion()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "created_at": fila[1],
                                "id_user": fila [2],
                                "ids:user": fila[3],
                                "nombre" : fila[4],
                                "description" : fila[5],
                                "Creation_date" : fila[6],
                                "update_date": fila[7],
                                "estado" : fila[8]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")


@router.get("/modalidad/buscar")
async def Obtener_datos(nombre: str):
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.filtrar_modalidad_operacion_por_nombre(nombre)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "created_at": fila[1],
                                "id_user": fila [2],
                                "ids:user": fila[3],
                                "nombre" : fila[4],
                                "description" : fila[5],
                                "Creation_date" : fila[6],
                                "update_date": fila[7],
                                "estado" : fila[8]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")


@router.delete("/modalidad/borrar")
async def eliminar_modalidad(id: str):
    conn.delete_modalidad_operacion(id)
    return {"message": f"Entrada con ID {id} eliminada correctamente"}


@router.post("/actualizar_estado")
async def actualizar_estado(ud: updateApp):
    try:
        conn.actualizar_modalidad_operacion(ud.estado,ud.id)

        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))



@router.post("/agregar/centro_operacion")
async def Agregar_centro_operacion(body: CentroOperacion):
    try:
        data = body.dict()
        conn.agregar_centro_operacion(data)

        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    


@router.get("/ver/centro_operacion")
async def get_centro_operacion(id_op : int):
     # Consulta SQL para obtener datos (por ejemplo)
    results = conn.mostrar_centros_operacion(id_op)


    return centro_operacion_schema(results)

@router.get("/ver/centro_operacion/asignado")
async def get_centro_operacion_asigando_a_vehiculo(id_op : int, id_ppu : int):
     # Consulta SQL para obtener datos (por ejemplo)
    results = conn.mostrar_centros_operacion_asignado_a_vehiculo(id_op,id_ppu)
    return centro_operacion_asignado_schema(results)