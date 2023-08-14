from fastapi import APIRouter, status,HTTPException, UploadFile, File
from typing import List
import pandas as pd
import os 

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

@router.get("/quadminds" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds():
    results = conn.get_cargas_quadmind()

    return cargas_quadminds_schema(results)


@router.get("/quadminds/pedidos_planificados" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_quadminds():
    results = conn.get_pedidos_planificados_quadmind()

    print(len(results))

    return pedidos_planificados_schema(results)

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
        # print(f"codigo cliente : {data['Código cliente']}, producto : {data['Producto']}, codigo pedido : {data['Codigo de Pedido']}")
        direccion = data['Domicilio']
        posicion = i + 1
        # conn.write_pedidos_planificados(data ,posicion, direccion)

        print(posicion)

    fecha_hora_actual = datetime.now()

    fecha_dia = fecha_hora_actual.strftime("%Y%m%d")

    fecha_hora_formateada = fecha_hora_actual.strftime("%Y%m%d%H%M")

    # error = conn.asignar_ruta_quadmind_manual(id_usuario, fecha_hora_formateada)
    error = [(1,"")]

    diferencia = conn.calcular_diferencia_tiempo(fecha_dia)

    # error 1 : codigos inexistentes

    if error[0][0] == 1:
        return {"filename": file.filename, 
                "message": "Error al subir el archivo", 
                "codigos": f"{error[0][1]}",
                "tiempo": diferencia[0][0],
                "termino" : True ,
                "error" : 1,
                "ruta" : ruta,
                "lista" : lista[180]
                }
    else:
        return {"filename": file.filename, 
                "message": error[0][1], 
                "codigos": "",
                "tiempo": diferencia[0][0],
                "termino" : True,
                "error" : 0,
                "ruta" : ruta,
                "lista" : lista[180]
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



