from fastapi import APIRouter, status,HTTPException, UploadFile, File
from typing import List
import pandas as pd
import os
import time

from datetime import datetime
##Modelos

from database.models.retiro_cliente import RetiroCliente
from database.schema.cargas.quadminds import cargas_quadminds_schema , cargas_quadminds_tuple_schema
from database.models.cargas.quadmind import CargaQuadmind


from database.schema.cargas.pedidos_planificados import pedidos_planificados_schema

from database.models.ruta_manual import RutaManual
from database.schema.ruta_manual import convert_to_json, rutas_manuales_schema

##Conexiones
from database.client import reportesConnection
from fastapi.responses import FileResponse
from openpyxl import Workbook
from openpyxl.styles import Font , PatternFill, Border ,Side


router = APIRouter(tags=["Cargas"], prefix="/api/cargas")

conn = reportesConnection()

quadeasy = []


@router.get("/quadminds/easy_cd" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds_easy_cd():
    results = conn.get_cargas_quadmind_easy_cd_mio()
    return cargas_quadminds_schema(results)

## este es con los datos de la db

@router.get("/quadminds/easy_cd/query" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds_easy_cd():
    results = conn.get_cargas_quadmind_easy_cd()
    return cargas_quadminds_schema(results)

@router.get("/quadminds/easy_opl" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds_easy_opl():

    ## es la version con WITH
    results = conn.get_cargas_quadmind_easy_opl_mio()

    return cargas_quadminds_schema(results)

@router.get("/quadminds/electrolux" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds_electrolux():
    results = conn.get_cargas_quadmind_electrolux()
    return cargas_quadminds_schema(results)

@router.get("/quadminds/sportex" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds_sportex():
    results = conn.get_cargas_quadmind_sportex()
    return cargas_quadminds_schema(results)

@router.get("/quadminds/retiro_tienda" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds_retiro_tienda():
    results = conn.get_cargas_quadmind_retiro_cliente()
    return cargas_quadminds_schema(results)


@router.get("/quadminds" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds():
    results = conn.get_cargas_quadmind()
    return cargas_quadminds_schema(results)

@router.get("/quadminds/limit" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds_offset(offset : int):
    results = conn.get_cargas_quadmind_offset(offset)
    return cargas_quadminds_schema(results)

@router.get("/quadminds/pedidos_planificados" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds():
    results = conn.get_pedidos_planificados_quadmind()
    return pedidos_planificados_schema(results)

@router.get("/quadminds/buscar/pedido_planificados" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds():
    results = conn.get_pedido_planificados_quadmind_by_cod_pedido()

    return results[0]

@router.post("/quadminds/subir-archivo", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo(id_usuario : str, file: UploadFile = File(...)):
    directorio  = os.path.abspath("excel")

    ruta = os.path.join(directorio,file.filename)

    with open(ruta, "wb") as f:
        contents = await file.read()
        print("pase por aqui")
        f.write(contents)

    df = pd.read_excel(ruta,skiprows=4)

    lista = df.to_dict(orient='records')

    for i, data in enumerate(lista):

        direccion = data['Domicilio']
        posicion = i + 1
        conn.write_pedidos_planificados(data ,posicion, direccion)
        print(posicion)


    fecha_hora_actual = datetime.now()

    fecha_dia = fecha_hora_actual.strftime("%Y%m%d")

    fecha_hora_formateada = fecha_hora_actual.strftime("%Y%m%d%H%M")
    
    error = conn.asignar_ruta_quadmind_manual(id_usuario, fecha_hora_formateada)

    diferencia = conn.calcular_diferencia_tiempo(fecha_dia)

    # error 1 : codigos inexistentes

    if error[0][0] == 1:
        return {
                "filename": file.filename, 
                "message": "Error al subir el archivo", 
                "codigos": f"{error[0][1]}",
                "tiempo": diferencia[0][0],
                "termino" : True ,
                "error" : 1,
                }
    else:   
        return {"filename": file.filename, 
                "message": error[0][1], 
                "codigos": "",
                "tiempo": diferencia[0][0],
                "termino" : True,
                "error" : 0,
                }

@router.post('/quadminds/asignar')
async def asignar_ruta(id_usuario : int):
    try:
        result = conn.asignar_ruta_quadmind_manual(id_usuario)

        return {
            "id_usuario" : id_usuario,
            "message" : "Ruta asignada Correctamente",
            "resultado" : result
        }
    except:
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Error")

@router.get('/hora_actual')
async def asignar_ruta():
    fecha_hora_actual = datetime.now()

    fecha_dia = fecha_hora_actual.strftime("%Y%m%d")

    fecha_hora_formateada = fecha_hora_actual.strftime("%Y%m%d%H%M")

    return {
        "fecha_dia" : fecha_dia,
        "fecha_hora_formateada": fecha_hora_formateada
    }