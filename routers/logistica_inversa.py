from fastapi import APIRouter, status,HTTPException

##Modelos 
from database.models.recepcion.recepcion_tiendas import bodyUpdateVerified
from database.models.retiro_cliente import RetiroCliente
from database.schema.log_inversa.estados import estados_schema,subestados_schema , estado_productos_schema
from database.models.log_inversa.bitacora_lg import BitacoraLg
from openpyxl.worksheet.page import PageMargins , PrintPageSetup
import lib.excel_generico as excel


## Schema
from database.schema.log_inversa.ruta_producto import ruta_productos_schema
from database.schema.log_inversa.lista_productos import lista_productos_schema
from database.schema.log_inversa.pendientes_dia import pendientes_schema
from database.schema.log_inversa.bodega_virtual import bodega_virtual_schema

## Modelos
from database.models.log_inversa.pendientes import PedidosPendientes
from database.models.log_inversa.ruta_producto import BodyRutaProducto
from database.models.log_inversa.reingreso_operacion import ReingresoOperacion

##Conexiones
from database.client import reportesConnection
from database.hela_prod import HelaConnection

from typing import List

router = APIRouter(tags=["LogisticaInversa"], prefix="/api/log_inversa")

conn = reportesConnection()

connHela = HelaConnection()

@router.post("/registrar", status_code=status.HTTP_202_ACCEPTED)
async def registrar_retiro_clientes(bitacora : BitacoraLg):
    # try:
        bitacora.Origen_registro = "Edición Pendientes"

        if bitacora.Estado_final == 0:
            bitacora.Subestado_final = None

        datos_orden = conn.get_ruta_manual(bitacora.Codigo_pedido)

        bitacora.Codigo_producto = datos_orden[0][14]

        data = bitacora.dict()

        conn.inser_bitacora_log_inversa(data)
        rows = conn.update_estados_pendientes(bitacora.Estado_final,bitacora.Subestado_final,bitacora.Codigo_pedido)

        # print(rows)
         
        return {
            "message" : "Orden de compra actualizada correctamente"
        }

    # except:
    #      raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Error")
    
## Estados y subestados full
@router.get("/estados", status_code=status.HTTP_202_ACCEPTED)
async def estados_entregas():

    estado = conn.obtener_estados_entrega()
    list_estado = estados_schema(estado)

    subestado = conn.obtener_subestados_entrega()
    list_subestado = subestados_schema(subestado)

    json = {
        "estado" : list_estado,
        "subestado" : list_subestado,
    }

    return json


##Estados y subestados Logistica Inversa

@router.get("/estados/li", status_code=status.HTTP_202_ACCEPTED)
async def estados_entregas_logistica_inversa():

    estado = conn.obtener_estados_entrega()
    list_estado = estados_schema(estado)

    subestado = conn.obtener_subestados_entrega_log_inversa()
    list_subestado = subestados_schema(subestado)

    json = {
        "estado" : list_estado,
        "subestado" : list_subestado,
    }

    return json

def cambiar_bool(valor):
     if valor is True:
          return "x"
     else:
          return ""

@router.post("/pendientes/descargar")
async def obtener_catalogo_rsv(pendientes : List[PedidosPendientes]):

    tupla = [( datos_envio.Origen, datos_envio.Cod_entrega, datos_envio.Fecha_ingreso, datos_envio.Fecha_compromiso, 
               datos_envio.Region, datos_envio.Comuna, datos_envio.Descripcion, datos_envio.Bultos, datos_envio.Estado, datos_envio.Subestado,
               cambiar_bool(datos_envio.Verificado), cambiar_bool(datos_envio.Recibido)) for datos_envio in pendientes]

    nombre_filas = ( 'Origen', 'Cod. Entrega', "Fecha Ingreso", "Fecha Compromiso", 
                     "Region", "Comuna","Descripcion","Bultos","Estado","Subestado",
                     "Verificado", "Recibido" )
    nombre_excel = f"Resumen_pendientes"

    return excel.generar_excel_generico(tupla,nombre_filas,nombre_excel)



@router.post("/ruta/buscar/producto",status_code=status.HTTP_202_ACCEPTED)
async def get_ruta_manual(body : bodyUpdateVerified):
    results  = conn.obtener_rutas_productos(body.cod_pedido)

    if len(body.cod_pedido) > 20:
        cod_opl = conn.get_codigo_pedido_opl(body.n_guia)[0][0]
        body.n_guia = cod_opl
        body.cod_pedido = cod_opl
        body.cod_producto = cod_opl


    # print(body)

    if results is None or results == []:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="El codigo del producto no existe")
    
    data = body.dict()
    # connHela.insert_data_bitacora_recepcion(data)

   

    return ruta_productos_schema(results)


@router.post("/ruta/buscar/nombre",status_code=status.HTTP_202_ACCEPTED)
async def get_ruta_manual(body : bodyUpdateVerified):
    results  = conn.obtener_rutas_productos_por_ruta(body.cod_pedido)

    # print(body)

    if results is None or results == []:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="El codigo del producto no existe")
    
    data = body.dict()
    # connHela.insert_data_bitacora_recepcion(data)

   

    return ruta_productos_schema(results)



@router.get("/ruta",status_code=status.HTTP_202_ACCEPTED)
async def get_ruta_manual(nombre_ruta : str):
    datos  = conn.get_lista_productos_ruta(nombre_ruta,"=")
    resultados = lista_productos_schema(datos)

    entregados = [obj for obj in resultados if obj["Estado"] == "Entregado"]
    no_entregados = [obj for obj in resultados if obj["Estado"] != "Entregado"]


    # data = body.dict()

    # connHela.insert_data_bitacora_recepcion(data)

    return {
         "entregados" : entregados,
         "no_entregado" : no_entregados
    }


@router.get("/ruta/estados",status_code=status.HTTP_202_ACCEPTED)
async def get_estados_pedidos(cod_pedido : str):
    no_entregado  = conn.get_estados_productos_ruta(cod_pedido)

    return estado_productos_schema(no_entregado)


@router.get("/pendientes",status_code=status.HTTP_202_ACCEPTED)
async def get_pendientes_log_inversa(fecha : str):

    fecha = fecha.replace("-","")
    # print(fecha)
    no_entregado  = conn.pendientes_log_inversa(fecha)

    return pendientes_schema(no_entregado)



@router.get("/bodega-virtual",status_code=status.HTTP_202_ACCEPTED)
async def recuperar_bodega_virtual():
    result  = conn.recuperar_bodega_virtual()

    return bodega_virtual_schema(result)


# reingresa_producto_a_operacion


@router.post("/reingresar/operacion",status_code=status.HTTP_202_ACCEPTED)
async def recuperar_bodega_virtual(body : ReingresoOperacion):
    data = body.dict()

    # conn.reingresa_producto_a_operacion(data)

    return {
        'message': 'Producto reingresado a Operacion',
        "req" : body
    }




    
