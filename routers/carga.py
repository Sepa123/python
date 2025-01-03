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
    # print(len(results))
    return cargas_quadminds_schema(results)
    # time.sleep(14)
    # print(len(quadeasy))
    # return quadeasy

## este es con los datos de la db

@router.get("/quadminds/easy_cd/query" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds_easy_cd():
    results = conn.get_cargas_quadmind_easy_cd()
    # print(len(results))
    return cargas_quadminds_schema(results)

@router.get("/quadminds/easy_opl" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds_easy_opl():
    # results = conn.get_cargas_quadmind_easy_opl()
    ## es la version con WITH
    results = conn.get_cargas_quadmind_easy_opl_mio()
    # print(len(results))
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
    # print(len(results))
    return cargas_quadminds_schema(results)

@router.get("/quadminds/pedidos_planificados" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds():
    results = conn.get_pedidos_planificados_quadmind()

    # print(len(results))

    return pedidos_planificados_schema(results)

@router.get("/quadminds/buscar/pedido_planificados" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds():
    results = conn.get_pedido_planificados_quadmind_by_cod_pedido()

    # print(len(results))

    return results[0]

@router.post("/quadminds/subir-archivo", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo(id_usuario : str, file: UploadFile = File(...)):

    # select quadminds.convierte_en_ruta_manual(1,'202308021040');

    directorio  = os.path.abspath("excel")

    ruta = os.path.join(directorio,file.filename)

    with open(ruta, "wb") as f:
        contents = await file.read()
        print("pase por aqui")
        f.write(contents)

    df = pd.read_excel(ruta,skiprows=4)

    lista = df.to_dict(orient='records')

    for i, data in enumerate(lista):
        # cantidad_encontrada = conn.get_pedido_planificados_quadmind_by_cod_pedido()
        # if cantidad_encontrada[0] >= 1:
        #     print("Producto ya esta registrado") 
        # else:
        direccion = data['Domicilio']
        posicion = i + 1
        conn.write_pedidos_planificados(data ,posicion, direccion)
        print(posicion)


    fecha_hora_actual = datetime.now()

    fecha_dia = fecha_hora_actual.strftime("%Y%m%d")

    fecha_hora_formateada = fecha_hora_actual.strftime("%Y%m%d%H%M")
    
    # time.sleep(8)
   
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
# @router.post("/quadminds/descargar", status_code=status.HTTP_202_ACCEPTED)
# async def descargar_quadminds_excel(body : List[CargaQuadmind]):

#     results = body

#     tupla_data = body.dict()

#     print(tupla_data)

#     return results
    # wb = Workbook()
    # ws = wb.active
    # print("Descarga /quadminds/fecha_compromiso")
    # results.insert(0, ("","","","",""))
    # results.insert(1, ("Codigo de Cliente","Nombre","Calle y Número","Ciudad","Provincia/Estado","Latitud",
    #                    "Longitud","Teléfono con código de país","Email","Código de Pedido","Fecha de Pedido","Operación E/R",
    #                    "Código de Producto","Descripción del Producto","Cantidad de Producto","Peso","Volumen","Dinero",
    #                    "Duración min","Ventana horaria 1","Ventana horaria 2","Notas","Agrupador","Email de Remitentes","Eliminar Pedido Si - No - Vacío",
    #                    "Vehículo","Habilidades"))
    # # for row in results:
    # #     texto = row[2]
    # #     texto_limpio = re.sub(r'[\x01]', '', str(texto))
    # #     new_row = row[:2] + (texto_limpio,) + row[3:]
    # #     ws.append(new_row)

    # for col in ws.columns:
    #     max_length = 0
    #     column = col[0].column_letter# get column letter
    #     for cell in col:
    #         try:
    #             if len(str(cell.value)) > max_length:
    #                 max_length = len(str(cell.value))
    #         except:
    #             pass
    #     adjusted_width = (max_length + 2)
    #     ws.column_dimensions[column].width = adjusted_width

    # wb.save("excel/Carga_Quadminds.xlsx")

    # return FileResponse("excel/Carga_Quadminds.xlsx")



