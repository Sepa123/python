from fastapi import APIRouter, status,HTTPException

##Modelos 

from database.models.retiro_cliente import RetiroCliente
from database.schema.log_inversa.estados import estados_schema,subestados_schema
from database.models.log_inversa.bitacora_lg import BitacoraLg
from openpyxl.worksheet.page import PageMargins , PrintPageSetup
import lib.excel_generico as excel

## Modelos

from database.models.log_inversa.pendientes import PedidosPendientes

##Conexiones
from database.client import reportesConnection

from typing import List

router = APIRouter(tags=["LogisticaInversa"], prefix="/api/log_inversa")

conn = reportesConnection()

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