from fastapi import APIRouter, status,HTTPException

##Modelos 

from database.models.retiro_cliente import RetiroCliente
from database.schema.cargas.quadminds import cargas_quadminds_schema
from database.models.cargas.quadmind import CargaQuadmind

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


@router.post("/quadminds/descargar", status_code=status.HTTP_202_ACCEPTED)
async def descargar_quadminds_excel(body : CargaQuadmind):

    results = body

    return results
    # wb = Workbook()
    # ws = wb.active
    # print("Descarga /quadminds/fecha_compromiso")
    # results.insert(0, ("","","","",""))
    # results.insert(1, ("Codigo de Cliente","Nombre","Calle y Número","Ciudad","Provincia/Estado","Latitud",
    #                    "Longitud","Teléfono con código de país","Email","Código de Pedido","Fecha de Pedido","Operación E/R",
    #                    "Código de Producto","Descripción del Producto","Cantidad de Producto","Peso","Volumen","Dinero",
    #                    "Duración min","Ventana horaria 1","Ventana horaria 2","Notas","Agrupador","Email de Remitentes","Eliminar Pedido Si - No - Vacío",
    #                    "Vehículo","Habilidades"))
    # for row in results:
    #     texto = row[2]
    #     texto_limpio = re.sub(r'[\x01]', '', str(texto))
    #     new_row = row[:2] + (texto_limpio,) + row[3:]
    #     ws.append(new_row)

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

    # wb.save("Carga_Quadminds_Fecha_Compromiso_yyyymmdd-hh24miss.xlsx")

    # return FileResponse("Carga_Quadminds_Fecha_Compromiso_yyyymmdd-hh24miss.xlsx")



