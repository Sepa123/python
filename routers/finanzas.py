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