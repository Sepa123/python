from fastapi import APIRouter, status,HTTPException
from fastapi.responses import FileResponse
from openpyxl import Workbook
# import re
# import time

# from database.models.pedidos import Pedidos
from database.schema.pedidos import pedidos_schema

# from database.models.pedidos_compromiso_sin_despacho import PedidosCompromisoSinDespacho
from database.schema.pedidos_compromiso_sin_despacho import pedidos_compromiso_sin_despacho_schema

# from database.models.pedidos_sin_tienda import PedidosSinTienda
from database.schema.pedidos_sin_tienda import pedidos_sin_tienda_schema

# from database.models.pedidos_tienda_easy_opl import PedidosTiendaEasyOPL
from database.schema.pedidos_tiendas_easy_opl import pedidos_tiendas_easy_opl_schema

from database.models.pedidos_pendientes import PedidosPendientes
from database.schema.pedidos_pendientes import pedidos_pendientes_schema

from database.models.rutas.rango_fecha import RangoFecha
from database.schema.rutas.rutas_de_pendientes import rutas_de_pendientes_schema

from database.client import reportesConnection
from fastapi.responses import FileResponse

router = APIRouter(tags=["pedidos"], prefix="/api/pedidos")

conn = reportesConnection()




##

##

###  Pedidos Con Fecha de Compromiso sin Despacho

@router.get("/sin_despacho" , status_code=status.HTTP_202_ACCEPTED)
async def get_pedidos_sin_despacho():
    results = conn.read_pedido_compromiso_sin_despacho()

    return pedidos_compromiso_sin_despacho_schema(results)

@router.get("/sin_despacho/descargar", status_code=status.HTTP_202_ACCEPTED)
async def get_pedidos_sin_despacho_descarga():

    results = conn.read_pedido_compromiso_sin_despacho()
    # print(results)
    wb = Workbook()
    ws = wb.active
    
    results.insert(0, ("",))
    results.insert(1,('Origen', 'Cod. Entrega', "Fecha Ingreso", "Fecha Compromiso", 
                      "Region", "Comuna","Descripcion","Bultos","Estado","Subestado",
                      "Verificado", "Recibido"))
    # update
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

### Pedidos Beetrack TOTAL PEDIDOS, Entregados, No entregados y en ruta

@router.get("/", status_code=status.HTTP_202_ACCEPTED)
async def get_pedidos():
    results = conn.read_pedidos()
    return pedidos_schema(results)

### Pedidos  de tiendas

@router.get("/sin_tiendas", status_code=status.HTTP_202_ACCEPTED)
async def get_pedidos_sin_tienda():
    results = conn.read_pedidos_sin_tienda()
    return pedidos_sin_tienda_schema(results)

@router.get("/easy_opl", status_code=status.HTTP_202_ACCEPTED)
async def get_pedidos_tiendas_easy_opl():
    results = conn.read_pedidos_tiendas_easy_opl()
    return pedidos_tiendas_easy_opl_schema(results)

#### Pedidos beetrack pendientes Atrasados ,En fecha , Adelantados: 

@router.get("/pendientes/total", status_code=status.HTTP_202_ACCEPTED)
async def pedidos_pendientes_total():
    results = conn.read_pedidos_pendientes_total()
    return pedidos_pendientes_schema(results)

@router.get("/pendientes/entregados", status_code=status.HTTP_202_ACCEPTED)
async def pedidos_pendientes_total():
    results = conn.read_pedidos_pendientes_entregados()
    return pedidos_pendientes_schema(results)

@router.get("/pendientes/no_entregados", status_code=status.HTTP_202_ACCEPTED)
async def pedidos_pendientes_total():
    results = conn.read_pedidos_pendientes_no_entregados()
    return pedidos_pendientes_schema(results)

@router.get("/pendientes/en_ruta", status_code=status.HTTP_202_ACCEPTED)
async def pedidos_pendientes_total():
    results = conn.read_pedidos_pendientes_en_ruta()
    return pedidos_pendientes_schema(results)


@router.post("/pendientes")
async def get_rutas_de_pendientes_por_rango(body : RangoFecha):
    data = body.dict()
    results = conn.read_rutas_pendientes_rango_fecha(data)
    return rutas_de_pendientes_schema(results)


@router.get("/pendientes/test")
async def get_rutas_de_pendientes_por_rango(fecha_inicio, fecha_fin):
    try:
        if fecha_fin == 'null':
            fecha_fin = None

        if fecha_inicio == 'null':
            fecha_inicio = None
        
        body = RangoFecha(Fecha_inicio=fecha_inicio,Fecha_fin=fecha_fin)
        data = body.dict()
        results = conn.read_rutas_pendientes_rango_fecha(data)
        return rutas_de_pendientes_schema(results)
    except:
        print("error pedidos/pendientes")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se pudieron cargar los pendientes,por favor vuelva a cargar la pagina")
    # return body


@router.get("/pendientes")
async def get_rutas_de_pendientes_limitada(offset : int):
     try:
        print("pendientes limit",offset)
        result = conn.prueba_ty(offset)
        print("pendientes limit fin",offset)
        return rutas_de_pendientes_schema(result)
     except:
        print("error pedidos/pendientes")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se pudieron cargar los pendientes,por favor vuelva a cargar la pagina")
   
@router.get("/pendientes/fechas")
async def get_rutas_de_pendientes_limitada():
     try:
        result = conn.fechas_pendientes()
        return {
            "Fecha_inicio" : result[0],
            "Fecha_fin" : result[1]
        }
     except Exception as e:
        print(e)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se pudieron cargar los pendientes,por favor vuelva a cargar la pagina")

@router.get("/pendientes/sportex-electrolux")
async def get_rutas_de_pendientes_limitada(fecha_inicio, fecha_fin):
     try:
        result = conn.pendientes_sportex_elux(fecha_inicio, fecha_fin)
        return rutas_de_pendientes_schema(result)
     except:
        print("error pedidos/pendientes")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se pudieron cargar los pendientes,por favor vuelva a cargar la pagina")
     


@router.get("/pendientes/easy_opl")
async def get_rutas_de_pendientes_limitada(fecha_inicio, fecha_fin , offset):
     try:
        result = conn.pendientes_easy_opl(fecha_inicio, fecha_fin, offset)
        print(len(result))
        return rutas_de_pendientes_schema(result)
     except:
        print("error pedidos/pendientes")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se pudieron cargar los pendientes,por favor vuelva a cargar la pagina")
     

@router.get("/pendientes/easy_opl/mio")
async def get_rutas_de_pendientes_limitada(fecha_inicio, fecha_fin , offset):
     try:
        result = conn.pendientes_easy_opl_mio(fecha_inicio, fecha_fin, offset)
        print(len(result))
        return rutas_de_pendientes_schema(result)
     except:
        print("error pedidos/pendientes")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se pudieron cargar los pendientes,por favor vuelva a cargar la pagina")
     

@router.get("/pendientes/retiro_tienda")
async def get_rutas_de_pendientes_limitada(fecha_inicio, fecha_fin , offset):
     try:
        result = conn.pendientes_retiro_tienda(fecha_inicio, fecha_fin, offset)
        print(len(result))
        return rutas_de_pendientes_schema(result)
     except:
        print("error pedidos/pendientes")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se pudieron cargar los pendientes,por favor vuelva a cargar la pagina")
     


@router.get("/pendientes/easy_cd")
async def get_rutas_de_pendientes_limitada(fecha_inicio, fecha_fin , offset):
     try:
        result = conn.pendientes_easy_cd(fecha_inicio, fecha_fin, offset)
        print(len(result))
        return rutas_de_pendientes_schema(result)
     except:
        print("error pedidos/pendientes")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se pudieron cargar los pendientes,por favor vuelva a cargar la pagina")
     





