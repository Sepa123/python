from fastapi import APIRouter, status,HTTPException, UploadFile, File
from typing import List
import pandas as pd
##Modelos 

from database.models.retiro_cliente import RetiroCliente
from database.schema.cargas.quadminds import cargas_quadminds_schema , cargas_quadminds_tuple_schema
from database.models.cargas.quadmind import CargaQuadmind


from database.schema.cargas.pedidos_planificados import pedidos_planificados_schema

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

@router.post("/quadminds/subir-archivo")
async def subir_archivo(file: UploadFile = File(...)):
    # Aquí puedes procesar el archivo
    # Por ejemplo, podrías guardarlo en el servidor con un nombre único
    with open(f"excel/{file.filename}", "wb") as f:
        contents = await file.read()
        f.write(contents)

    df = pd.read_excel(f"excel/{file.filename}",skiprows=4)

    lista = df.to_dict(orient='records')
 
    for data in lista:
        print(data['Codigo de Pedido'])
  

    return {"filename": file.filename, "message": "Archivo subido exitosamente"}

@router.post('/quadminds/asignar')
async def asignar_ruta(id_usuario : int):
    try:
        conn.asignar_ruta_quadmind_manual(id_usuario)
        return {
            "message" : "Ruta asignada Correctamente"
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



