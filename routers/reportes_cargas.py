from fastapi import APIRouter,status,HTTPException
from database.client import reportesConnection
from fastapi.responses import FileResponse
from openpyxl import Workbook
import re ,json
from urllib.parse import unquote
# from os import remove
from database.models.producto_picking import producto_picking
from database.schema.producto_picking import producto_picking_schema, productos_picking_schema

from database.models.reporte_historico import ReporteHistorico
from database.schema.reporte_historico import reportes_historico_schema

from database.models.carga_easy import CargaEasy
from database.schema.cargas_easy import cargas_easy_schema

from database.schema.reporte_hora import reportes_hora_schema, reportes_ultima_hora_schema
from database.schema.reportes_easy_region import reportes_easy_region_schema

from database.models.reporte_productos_entregados import ReporteProducto
from database.schema.reporte_productos_entregados import reportes_producto_schema

from database.models.pedidos_compromiso_sin_despacho import PedidosCompromisoSinDespacho
from database.schema.pedidos_compromiso_sin_despacho import pedidos_compromiso_sin_despacho_schema

from database.schema.rutas_beetrack_hoy import rutas_beetrack_hoy_schema

from database.models.pedidos import Pedidos
from database.schema.pedidos import pedidos_schema

from database.models.pedidos_sin_tienda import PedidosSinTienda
from database.schema.pedidos_sin_tienda import pedidos_sin_tienda_schema

from database.models.pedidos_tienda_easy_opl import PedidosTiendaEasyOPL
from database.schema.pedidos_tiendas_easy_opl import pedidos_tiendas_easy_opl_schema

from database.models.pedidos_pendientes import PedidosPendientes
from database.schema.pedidos_pendientes import pedidos_pendientes_schema

from database.models.carga_easy_comparacion import CargaEasyComparacion
from database.schema.carga_easy_comparacion import cargas_easy_comparacion_schema


from database.models.operaciones.nro_cargas_hora import NroCargasHora
from database.schema.operaciones.nro_cargas_hora import nro_cargas_hora_schema

from database.schema.cargas.beetrack_rango import beetrack_rango_schema
from database.models.ns_valor_ruta import asignarValor

from database.models.ns_valor_ruta import asignarValor

from typing import List
from fastapi.params import Query

router = APIRouter(tags=["reportes"],prefix="/api/reportes")

conn = reportesConnection()

#asignar valor a la ruta existente
@router.put("/NS_beetrack/rango",status_code=status.HTTP_202_ACCEPTED)
async def update_beetrack_valor_ruta(body: List[asignarValor]):
    output = conn.update_valor_rutas(body)
    print(body)
    return { "message":f"Valor agregado correctamente "}


@router.get("/cargas_easy",status_code=status.HTTP_202_ACCEPTED)
async def get_cuenta():
    data_db = conn.read_cargas_easy()

    if not data_db:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")
    
    print(" /cargas_easy/")
    return cargas_easy_schema(data_db)

### Quadminds
@router.get("/clientes",status_code=status.HTTP_202_ACCEPTED)
async def get_data_cliente():
    results = conn.read_clientes()
    # print(results)
    wb = Workbook()
    ws = wb.active
    print(" /quadminds/")
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

@router.get("/clientes/json",status_code=status.HTTP_202_ACCEPTED)
async def get_data_cliente():
    results = conn.read_clientes()
    print(len(productos_picking_schema(results)))
    return productos_picking_schema(results)

@router.get("/quadminds/fecha_compromiso",status_code=status.HTTP_202_ACCEPTED )
async def get_quadminds_fecha_compromiso():
    results = conn.read_reporte_quadmind_fecha_compromiso()
    # print(results)
    wb = Workbook()
    ws = wb.active
    print("Descarga /quadminds/fecha_compromiso")
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

    wb.save("Carga_Quadminds_Fecha_Compromiso_yyyymmdd-hh24miss.xlsx")

    return FileResponse("Carga_Quadminds_Fecha_Compromiso_yyyymmdd-hh24miss.xlsx")

@router.get("/quadminds/fecha_compromiso/json",status_code=status.HTTP_202_ACCEPTED)
async def get_data_cliente():
    results = conn.read_reporte_quadmind_fecha_compromiso()
    print(len(productos_picking_schema(results)))
    return productos_picking_schema(results)

@router.get("/quadminds/tamano",status_code=status.HTTP_202_ACCEPTED )
async def get_quadminds_fecha_compromiso():
    results = conn.read_resumen_quadmind_tamano()
    # print(results)
    wb = Workbook()
    ws = wb.active
    print("Descarga /quadminds/tamano")
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

    wb.save("excel/Carga_Quadminds_tamano_yyyymmdd-hh24miss.xlsx")

    return FileResponse("excel/Carga_Quadminds_tamano_yyyymmdd-hh24miss.xlsx")


@router.get("/NS_beetrack_Mensual",status_code=status.HTTP_202_ACCEPTED)
async def get_beetrack_mensual():
    results = conn.read_NS_beetrack_mensual()

    wb = Workbook()
    ws = wb.active
    print("/NS_beetrack_Mensual")
    results.insert(0, ("",))
    results.insert(1,('FECHA', 'ID. RUTA', 'DRIVER', 'PATENTE', 'REGION', 'Km. Ruta', 'T-PED', 'Easy', 'Electrolux', 'Sportex', 'Imperial', 'PBB', 'Virutex', 'R1', 'R2', 'R3', 'VR', 'C11', '(%) 11', 'C13', '(%) 13', 'C15', '(%)15', 'C17', '(%)17', 'C18', '(%)18', 'C20', '(%)20', 'Final_D', 'OBSERV-RUTA', 'H_INIC', 'H_TERM', 'TT-RUTA', 'Prom. ENT', 'T-ENT', 'N-ENT', 'EE', 'SM', 'CA', 'DA', 'RxD', 'DNE', 'DNCC', 'D.ERR', 'INC.T', 'DFORM', 'PINCOM', 'SPELI', 'PNCORR', 'PFALT', 'PPARC', 'P.DUPL', 'R', 'Pedidos'))

    for row in results:

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

@router.get("/NS_beetrack/rango",status_code=status.HTTP_202_ACCEPTED)
async def get_beetrack_rango(fecha_inicio: str, fecha_fin : str):
    results = conn.get_NS_beetrack_por_rango_fecha(fecha_inicio,fecha_fin)
    return beetrack_rango_schema(results)

#asignar valor a la ruta existente
@router.put("/NS_beetrack/rango/{Id_ruta}",status_code=status.HTTP_202_ACCEPTED)
async def update_beetrack_valor_ruta(body :asignarValor):

    conn.update_valor_rutas(body.Valor_ruta,body.Id_ruta)
    return { "message":f"Valor agregado correctamente {body.Valor_ruta}"}


@router.get("/NS_beetrack/rango/descargar",status_code=status.HTTP_202_ACCEPTED)
async def get_beetrack_rango(fecha_inicio: str, fecha_fin : str):
    results = conn.get_NS_beetrack_por_rango_fecha(fecha_inicio,fecha_fin)
    # beetrack_dict = beetrack_rango_schema(results) 
    wb = Workbook()
    ws = wb.active
    
    results.insert(0, ("",))
    results.insert(1,("FECHA", "ID. RUTA","DRIVER","PATENTE","REGION","Km. Ruta","T-PED","Easy","Electrolux","Sportex","Imperial","PBB","Virutex","R1","R2","R3","VR",
    "C11","(%) 11","C13","(%) 13","C15","(%)15","C17","(%)17","C18","(%)18","C20","(%)20","Final_D","OBSERV-RUTA","H_INIC","H_TERM","TT-RUTA","Prom. ENT","T-ENT",
    "N-ENT","EE","SM","CA","DA","RxD","DNE","DNCC","D.ERR","INC.T","DFORM","PINCOM","SPELI","PNCORR","PFALT","PPARC","P.DUPL","R","Pedidos", "Valor Ruta"
))
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
    wb.save("excel/NS_beetrack_rango.xlsx")

    return FileResponse("excel/NS_beetrack_rango.xlsx")


## Reportes Historicos
@router.get("/historico/mensual",status_code=status.HTTP_202_ACCEPTED)
async def get_historico_mensual():
    results = conn.read_reporte_historico_mensual()
    print("/historico/mensual")
    return reportes_historico_schema(results)

@router.get("/historico/hoy",status_code=status.HTTP_202_ACCEPTED)
async def get_historico_hoy():
    results = conn.read_reporte_historico_hoy()
    print("/historico/hoy")
    return reportes_historico_schema(results)

@router.get("/historico/anual",status_code=status.HTTP_202_ACCEPTED)
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

@router.get("/productos/mensual",status_code=status.HTTP_202_ACCEPTED)
async def get_productos_mensual():
    results = conn.read_reporte_producto_entregado_mensual()
    print("/producto/mensual")
    return reportes_producto_schema(results)

@router.get("/productos/hoy",status_code=status.HTTP_202_ACCEPTED)
async def get_productos_hoy():
    results = conn.read_reporte_producto_entregado_hoy()
    print("/productos/hoy")
    return reportes_producto_schema(results)

@router.get("/productos/rango",status_code=status.HTTP_202_ACCEPTED)
async def get_productos_por_rango_fecha(inicio, termino):
    print("inicio",inicio,"termino",termino)

    if inicio == 'undefined' or termino == 'undefined':
    # Una o ambas variables son None
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="las varaibles no estan definidas")
  
    results = conn.read_reporte_producto_entregado_por_rango_fecha(inicio,termino)
    
    print("/productos/rango")
    return reportes_producto_schema(results)

## TODO: EL ANUAL ES PARA DESCARGA
@router.get("/productos/anual",status_code=status.HTTP_202_ACCEPTED)
async def get_productos_anual():
    results = conn.read_reporte_producto_entregado_anual()
    return reportes_producto_schema(results)

# Cantidad de Entregas por hora
@router.get("/hora",status_code=status.HTTP_202_ACCEPTED)
async def get_reportes_hora():
    results = conn.read_reportes_hora()
    print("reporte/hora")
    return reportes_hora_schema(results)

@router.get("/ultima_hora",status_code=status.HTTP_202_ACCEPTED)
async def get_reportes_ultima_hora():
    results = conn.read_reporte_ultima_hora()
    return reportes_ultima_hora_schema(results)


@router.get("/productos/easy_region",status_code=status.HTTP_202_ACCEPTED)
async def get_productos_easy_region():
    results = conn.read_productos_easy_region()
    print("/productos/easy_region")
    return reportes_easy_region_schema(results)

@router.get("/pedidos/sin_despacho",status_code=status.HTTP_202_ACCEPTED)
async def get_pedidos_sin_despacho():
    results = conn.read_pedido_compromiso_sin_despacho()
    print("/pedidos/sin_despacho") 
    return pedidos_compromiso_sin_despacho_schema(results)

@router.get("/pedidos/sin_despacho/descargar",status_code=status.HTTP_202_ACCEPTED)
async def get_pedidos_sin_despacho_descarga():

    results = conn.read_pedido_compromiso_sin_despacho()

    wb = Workbook()
    ws = wb.active
    print("/pedidos/sin_despacho/descargar")
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

@router.get("/pedidos",status_code=status.HTTP_202_ACCEPTED)
async def get_pedidos():
    results = conn.read_pedidos()
    print("/pedidos")
    return pedidos_schema(results)

@router.get("/ruta/beetrack/hoy",status_code=status.HTTP_202_ACCEPTED)
async def get_ruta_beetrack_hoy():
    results = conn.read_ruta_beetrack_hoy()
    print("/ruta/beetrack/hoy")
    return rutas_beetrack_hoy_schema(results)

@router.get("/pedidos/sin_tiendas",status_code=status.HTTP_202_ACCEPTED)
async def get_pedidos_sin_tienda():
    results = conn.read_pedidos_sin_tienda()
    print("/pedidos/sin_tiendas")
    return pedidos_sin_tienda_schema(results)

@router.get("/pedidos/easy_opl",status_code=status.HTTP_202_ACCEPTED)
async def get_pedidos_tiendas_easy_opl():
    results = conn.read_pedidos_tiendas_easy_opl()
    print("/pedidos/easy_opl")
    return pedidos_tiendas_easy_opl_schema(results)

# @router.get("/timezone",status_code=status.HTTP_202_ACCEPTED)
# async def get_pedidos_tiendas_easy_opl():
#     results = conn.get_timezone()
#     return results

@router.get("/pedidos/pendientes/total",status_code=status.HTTP_202_ACCEPTED)
async def pedidos_pendientes_total():
    results = conn.read_pedidos_pendientes_total()
    return pedidos_pendientes_schema(results)

@router.get("/pedidos/pendientes/entregados",status_code=status.HTTP_202_ACCEPTED)
async def pedidos_pendientes_total():
    results = conn.read_pedidos_pendientes_entregados()
    return pedidos_pendientes_schema(results)

@router.get("/pedidos/pendientes/no_entregados",status_code=status.HTTP_202_ACCEPTED)
async def pedidos_pendientes_total():
    results = conn.read_pedidos_pendientes_no_entregados()
    return pedidos_pendientes_schema(results)

@router.get("/pedidos/pendientes/en_ruta",status_code=status.HTTP_202_ACCEPTED)
async def pedidos_pendientes_total():
    results = conn.read_pedidos_pendientes_en_ruta()
    return pedidos_pendientes_schema(results)

## Comparacion cargas easy API VS WMS

@router.get("/cargas_easy/api",status_code=status.HTTP_202_ACCEPTED)
async def cargas_easy_api():
    results = conn.read_carga_easy_api()
    return cargas_easy_comparacion_schema(results)

@router.get("/cargas_easy/wms",status_code=status.HTTP_202_ACCEPTED)
async def cargas_easy_api():
    results = conn.read_carga_easy_wms()
    return cargas_easy_comparacion_schema(results)

#producto picking

@router.get("/buscar/producto")
async def productos_picking():
    results = conn.get_producto_picking()
    return producto_picking_schema(results)

@router.get("/buscar/producto/{producto_id}",status_code=status.HTTP_202_ACCEPTED)
async def producto_picking_id(producto_id : str):
    results = conn.get_producto_picking_id(producto_id)
    # print(results[""])
    print("/buscar/producto/")
    if results is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="El codigo del producto no existe")
    return producto_picking_schema(results)


# cargas por hora

@router.get("/cargas_por_hora",status_code=status.HTTP_202_ACCEPTED)
async def cargas_por_hora():
    results = conn.get_carga_hora()

    return nro_cargas_hora_schema(results)