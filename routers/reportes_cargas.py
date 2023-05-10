from fastapi import APIRouter,status,HTTPException
from database.client import reportesConnection
from datetime import datetime
from fastapi.responses import FileResponse
from openpyxl import Workbook, load_workbook

from database.models.reporte_historico import ReporteHistorico
from database.schema.reporte_historico import reportes_historico_schema, reporte_historico_schema
import re
from os import remove
from database.models.reportes import cargaEasy_schema

router = APIRouter(prefix="/api/reportes")

conn = reportesConnection()

@router.get("/cargas_easy")
async def get_cuenta():
    data_db = conn.read_cargas_easy()
    if not data_db:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")
    return cargaEasy_schema(data_db)

@router.get("/clientes")
async def get_data_cliente():
    results = conn.read_clientes()
    # print(results)
    wb = Workbook()
    ws = wb.active

    results.insert(0, ("","","","",""))
    results.insert(1, ("Codigo de Cliente","Nombre","Calle y Número","Ciudad","Provincia/Estado","Latitud",
                       "Longitud","Teléfono con código de país","Email","Código de Pedido","Fecha de Pedido","Operación E/R",
                       "Código de Producto","Descripción del Producto","Cantidad de Producto","Peso","Volumen","Dinero",
                       "Duración min","Ventana horaria 1","Ventana horaria 2","Notas","Agrupador","Email de Remitentes","Eliminar Pedido Si - No - Vacío",
                       "Vehículo","Habilidades"))


    for row in results:
        texto = row[2]
        texto_limpio = re.sub(r'[\x01]', '', str(texto))
        new_row = row[:2] + (texto_limpio,) + row[3:]
        print(new_row[2])
        ws.append(new_row)

    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter# get column letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column].width = adjusted_width

    wb.save("Carga_Quadminds_yyyymmdd-hh24miss.xlsx")

    return FileResponse("Carga_Quadminds_yyyymmdd-hh24miss.xlsx")

@router.get("/NS_beetrack_Mensual")
async def get_beetrack_mensual():
    results = conn.read_NS_beetrack_mensual()
    # print(results)
    wb = Workbook()
    ws = wb.active
    # plantilla = load_workbook(filename="excel/NS_Beetrack_Mensual_plantilla.xlsx")
    # sheet = plantilla.active
    # row_num = 3
    # for row in results:
    #     col_num = 1
    #     for value in row:
    #         sheet.cell(row=row_num, column=col_num, value=value)
    #         col_num += 1
    #     row_num += 1

    # plantilla.save(filename="archivo.xlsx")

    results.insert(0, ("",))
    results.insert(1,('FECHA', 'ID. RUTA', 'DRIVER', 'PATENTE', 'REGION', 'Km. Ruta', 'T-PED', 'Easy', 'Electrolux', 'Sportex', 'Imperial', 'PBB', 'Virutex', 'R1', 'R2', 'R3', 'VR', 'C11', '(%) 11', 'C13', '(%) 13', 'C15', '(%)15', 'C17', '(%)17', 'C18', '(%)18', 'C20', '(%)20', 'Final_D', 'OBSERV-RUTA', 'H_INIC', 'H_TERM', 'TT-RUTA', 'Prom. ENT', 'T-ENT', 'N-ENT', 'EE', 'SM', 'CA', 'DA', 'RxD', 'DNE', 'DNCC', 'D.ERR', 'INC.T', 'DFORM', 'PINCOM', 'SPELI', 'PNCORR', 'PFALT', 'PPARC', 'P.DUPL', 'R', 'Pedidos'))

    for row in results:
        # print(row)
        ws.append(row)

    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter# get column letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column].width = adjusted_width
    results.insert(0, ("",))
    wb.save("excel/NS_Beetrack_Mensual.xlsx")

    return FileResponse("excel/NS_Beetrack_Mensual.xlsx")
    

@router.get("/historico/mensual")
async def get_historico_mensual():
    results = conn.read_reporte_historico_mensual()
    # print(conn.read_reporte_historico_mensual())
    return reportes_historico_schema(results)

@router.get("/historico/anual")
async def get_historico_mensual():
    results = conn.read_reporte_historico_anual()
    # print(conn.read_reporte_historico_mensual())
    return reportes_historico_schema(results)

# async def list_historico_mensual():
#     return reportes_historico_schema(conn.read_reporte_historico_mensual())

