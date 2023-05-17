from fastapi import APIRouter,status,HTTPException
from database.client import reportesConnection
from datetime import datetime
from fastapi.responses import FileResponse
from openpyxl import Workbook
import re
from os import remove

from database.models.reporte_historico import ReporteHistorico
from database.schema.reporte_historico import reportes_historico_schema, reporte_historico_schema
from database.models.reportes import cargaEasy_schema

from database.schema.reporte_hora import reportes_hora_schema
from database.schema.reportes_easy_region import reportes_easy_region_schema

from database.models.reporte_productos_entregados import ReporteProducto
from database.schema.reporte_productos_entregados import reportes_producto_schema

from database.models.pedidos_compromiso_sin_despacho import pedidos_compromiso_sin_despacho
from database.schema.pedidos_compromiso_sin_despacho import pedidos_compromiso_sin_despacho_schema

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
    
## Reportes Historicos
@router.get("/historico/mensual")
async def get_historico_mensual():
    results = conn.read_reporte_historico_mensual()
    # print(conn.read_reporte_historico_mensual())
    return reportes_historico_schema(results)

@router.get("/historico/anual")
async def get_historico_mensual():
    results = conn.read_reporte_historico_anual()

    wb = Workbook()
    ws = wb.active
    
    results.insert(0, ("",))
    results.insert(1,('Día', 'Fecha', 'Electrolux', 'Sportex', 'Easy', 'Tiendas'))
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
    wb.save("excel/Reporte_historico_mensual.xlsx")

    return FileResponse("excel/Reporte_historico_mensual.xlsx")

## reporte productos entregados

@router.get("/productos/mensual")
async def get_productos_mensual():
    results = conn.read_reporte_producto_entregado_mensual()
    return reportes_producto_schema(results)


## TODO: EL ANUAL ES PARA DESCARGA
@router.get("/productos/anual")
async def get_productos_anual():
    results = conn.read_reporte_producto_entregado_anual()
    # wb = Workbook()
    # ws = wb.active
    
    # results.insert(0, ("",))
    # results.insert(1,('Día', 'Fecha', 'Electrolux', 'Sportex', 'Easy', 'Tiendas', "Easy OPL"))
    # for row in results:
    #     # print(row)
    #     ws.append(row)

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
    # results.insert(0, ("",))
    # wb.save("excel/Reporte_producto_mensual.xlsx")

    # return FileResponse("excel/Reporte_producto_mensual.xlsx")
    return reportes_producto_schema(results)

@router.get("/hora")
async def get_reportes_hora():
    results = conn.read_reportes_hora()
    return reportes_hora_schema(results)


@router.get("/productos/easy_region")
async def get_productos_easy_region():
    results = conn.read_productos_easy_region()
    # print(results)
    return reportes_easy_region_schema(results)

@router.get("/pedidos/sin_despacho")
async def get_pedidos_sin_despacho():
    results = conn.read_pedido_compromiso_sin_despacho()

    return pedidos_compromiso_sin_despacho_schema(results)

@router.get("/pedidos/sin_despacho/descargar")
async def get_pedidos_sin_despacho_descarga():
    results = conn.read_pedido_compromiso_sin_despacho()

    wb = Workbook()
    ws = wb.active
    
    results.insert(0, ("",))
    results.insert(1,('Origen', 'Cod. Entrega', "Fecha Ingreso", "Fecha Compromiso", 
                      "Region", "Comuna","Descripcion","Bultos"))

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
    wb.save("excel/pedidos_con_fecha_de_compromiso_sin_despacho.xlsx")

    return FileResponse("excel/pedidos_con_fecha_de_compromiso_sin_despacho.xlsx")