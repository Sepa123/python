from fastapi import APIRouter, status,HTTPException
from typing import List
import re, json
import lib.excel_generico as excel

##Conexiones
from database.client import reportesConnection , UserConnection
from database.hela_prod import HelaConnection
from datetime import datetime

##Modelos Schemas



router = APIRouter(tags=["Finanzas"], prefix="/api/finanzas")

conn = reportesConnection()



@router.get("/infoTarifas")
async def Obtener_datos():
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_info_tarifario()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "nombre" : fila[0],
                                "valor_inferior": fila[1],
                                "valor_superior": fila [2],
                                "unidad": fila[3],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    


@router.get("/TipoUnidad")
async def Obtener_datos():
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_tipo_unidad()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "unidad": fila[1],
                                "campo_vehiculo": fila [2],
                                "prioridad": fila[3],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    


@router.post("/NuevaTarifa")
async def actualizar_estado(id_usuario:str, ids_usuario: str, nombre:str, valor_inferior:int, valor_superior:int, unidad:int):
    try:
        conn.agregar_nueva_tarifa(id_usuario, ids_usuario, nombre, valor_inferior, valor_superior, unidad)
        print()
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))



## Tarifario General

@router.get("/tarifarioGeneral/getOperacion")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.obtener_modalidad_operaciones()
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

@router.get("/tarifarioGeneral/infoTableToSearch")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.obtener_info_tarifario_general_null()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id": fila [0],
                                "operacion" : fila[1],
                                "centro_operacion": fila[2],
                                "tipo_vehiculo":fila[3],
                                "capacidad":fila[4],
                                "periodicidad":fila[5],
                                "tarifa":fila[6],
                                "fecha_de_caducidad":fila[7]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")


@router.get("/tarifarioGeneral/getCentroOperacion")
async def Obtener_datos(id_op: int):
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_centro_operacion(id_op)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "centro": fila[1],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioGeneral/getTipoVehiculo")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = f"""select * from transporte.tipo_vehiculo tv """
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_tipo_vehiculo()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "tipo": fila[1],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioGeneral/GetCaracteristicasTarifa")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.obtener_caract_finanzas()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "nombre": fila[1],
                                "valor_inferior":fila[2],
                                "valor_superior":fila[3],
                                "unidad": fila[4]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")


@router.get("/tarifarioGeneral/getPeriodicidad")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.obtener_periodicidad()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "periodo": fila[1],
                                "descripcion":fila[2],
                                "icono":fila[3]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioGeneral/getInfoTable")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.obtener_info_tarifario_general()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{  
                                "id": fila [0],
                                "operacion" : fila[1],
                                "centro_operacion": fila[2],
                                "tipo_vehiculo":fila[3],
                                "capacidad":fila[4],
                                "periodicidad":fila[5],
                                "tarifa":fila[6],
                                "fecha_de_caducidad":fila[7]


                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioGeneral/CentroFilter")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.obtener_centro_operacion_filter()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "centro": fila[1],
                                "descripcion": fila[2]

                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.post("/tarifarioGeneral/insertDate")
async def actualizar_estado(id:str, fecha_de_caducidad:str):
    try:
        conn.actualizar_fecha_tarifario_general(id, fecha_de_caducidad)
        print()
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))


@router.post("/tarifarioGeneral/NuevaTarifa")
async def actualizar_estado(id_usuario:str, ids_usuario:str, latitud:str, longitud:str, operacion:int, centro_operacion:int, tipo_vehiculo:int, capacidad:int, periodicidad: int, tarifa: int):
    try:
        conn.agregar_nuevo_tarifario_general(id_usuario, ids_usuario, latitud, longitud, operacion, centro_operacion, tipo_vehiculo, capacidad, periodicidad, tarifa)
        print()
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))