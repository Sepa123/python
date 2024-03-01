from fastapi import APIRouter, status,HTTPException
from typing import List
import re

##Conexiones
from database.client import reportesConnection , UserConnection
from database.hela_prod import HelaConnection
from datetime import datetime

##Modelos Schemas

from database.models.toc.toc_bitacora import BitacoraToc
from database.schema.toc.producto_toc import buscar_producto_toc_schema

from database.schema.toc.subestados import subestados_schema
from database.schema.toc.codigo1 import codigos1_schema

from database.schema.toc.observaciones_usuario import observaciones_usuario_schema

from database.schema.toc.bitacora_toc_usuarios import bitacoras_usuarios_schema

from database.schema.toc.alertas_vigentes import alertas_vigentes_schema

from database.schema.toc.bitacora_rango_fecha import bitacoras_rango_fecha_schema

from database.schema.toc.actividad_diaria import actividades_diaria_schema
from database.schema.toc.backoffice_usuario import backoffices_usuario_schema

from database.schema.rutas.toc_tracking import toc_tracking_schema
from database.schema.toc.buscar_alerta import buscar_alerta, buscar_alertas_schema

from database.models.toc.editar_toc import EditarTOC

from database.schema.toc.dif_fechas_easy import dif_fecha_easy_schema

router = APIRouter(tags=["TOC"], prefix="/api/toc")

conn = reportesConnection()
connHela = HelaConnection()
connUser = UserConnection()

@router.get("/buscar_producto/{cod_producto}")
async def buscar_producto(cod_producto : str):
    try:
        result = conn.buscar_producto_toc(cod_producto)
        return buscar_producto_toc_schema(result)
    except:
          print("error en toc//buscar_producto/cod_producto")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error, codigo no encontrado")

@router.get("/subestados", status_code=status.HTTP_202_ACCEPTED)
async def obtener_subestados():
     results = conn.buscar_subestados()
     return subestados_schema(results)

@router.get("/codigos1", status_code=status.HTTP_202_ACCEPTED)
async def obtener_subestados():
     results = conn.buscar_codigos1()
     return codigos1_schema(results)

@router.post("/registrar_bitacora", status_code=status.HTTP_201_CREATED)
async def buscar_producto(body : BitacoraToc):
#     try:
        conn.update_alerta_bitacora_toc_by_guia(body.Guia)
        if body.PPU == '':
             body.PPU = None

        if body.Guia == '':
             body.Guia = None
        
        if body.Cliente == '':
             body.Cliente = None

        if body.Region == '':
             body.Region = None
        
        if body.Estado == '':
             body.Estado = None
        
        if body.Subestado == '':
             body.Subestado = None

        if body.Driver == '':
             body.Driver = None

        if body.Nombre_cliente == '':
             body.Nombre_cliente = None

        current_date = datetime.now()
        formatted_date = current_date.strftime('%Y-%m-%d')

        if body.Codigo1Str == "" or body.Codigo1Str is None:
             body.Codigo1 = None
        else:
             body.Codigo1 = int(body.Codigo1Str)
          
        if body.Direccion_correcta == "":
             body.Direccion_correcta = None
        
        if body.Fecha_reprogramada == "":
             body.Fecha_reprogramada = None

        if body.Subestado_esperado == "":
             body.Subestado_esperado = None
        
        if body.Observacion == "":
             body.Observacion = None
        
        if body.Fecha_compromiso == "":
             body.Fecha_compromiso = formatted_date
        
        if body.Fecha == "":
             body.Fecha = formatted_date

        if body.Comuna == "":
             body.Comuna = None

        id_transyanez = conn.id_transyanez_bitacora()[0]
        body.Id_transyanez = id_transyanez
        if body.Comuna is None and body.Comuna_correcta is None :
             datos_faltantes = conn.get_comuna_por_ruta_manual(body.Guia)
             if datos_faltantes is not None:
                body.Comuna = datos_faltantes[0]
                body.Region = datos_faltantes[1]
             else:
                body.Comuna = 'Sin info.'
                body.Region = 'Sin info.'
                  
        body.Ids_transyanez = f"ty{id_transyanez}"


        data = body.dict()
        conn.insert_bitacora_toc(data)
        return {"message" : f"Bitacora {body.Ids_transyanez} registrada correctamente"}
#     except:
#         print("error")
#         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error al registrar la bitacora")
    

@router.get("/observaciones/{ids_usuario}")
async def get_observaciones_usuario(ids_usuario : str):
     results = conn.obtener_observaciones_usuario(ids_usuario)
     return observaciones_usuario_schema(results)

@router.get("/alertas-vigentes")
async def get_alertas_vigentes():
     results = conn.obtener_alertas_vigentes()
     return alertas_vigentes_schema(results)

@router.get("/bitacoras/usuarios")
async def get_bitacoras_usuarios(fecha_inicio : str, fecha_fin : str):
     results = conn.obtener_nombres_usu_toc(fecha_inicio,fecha_fin)
     pattern = r'\bportal-\b'

     bitacora_usuario = bitacoras_usuarios_schema(results)

     for usu in bitacora_usuario:
          if re.search(pattern,usu['Ids_usuario']) :
               id = usu['Ids_usuario'].replace("portal-","")
               nombre_usu = connUser.get_nombre_usuario(id)[0]
               usu['Nombre'] = nombre_usu
          else:
               id_hela = usu['Ids_usuario'].replace("hela-","")
               nombre_usu_hela = connHela.get_nombre_usuario_hela(id_hela)[0]
               usu['Nombre'] = nombre_usu_hela
     return bitacora_usuario

@router.get("/bitacoras/rango")
async def get_bitacoras_usuarios(fecha_inicio : str, fecha_fin : str):
     results = conn.bitacoras_rango_fecha(fecha_inicio,fecha_fin)
     return bitacoras_rango_fecha_schema(results)

@router.get("/usuario/portal/{id_usuario}")
async def get_bitacoras_usuarios(id_usuario : str):
     id = id_usuario.replace("portal-","")
     results = connUser.get_nombre_usuario(id)
     return results[0]

@router.get("/usuario/hela/{id_usuario}")
async def get_bitacoras_usuarios(id_usuario : str):
     id = id_usuario.replace("hela-","")
     results = connUser.get_nombre_usuario(id)
     return results[0]


@router.get("/actividad_diaria")
async def get_actividad_diaria_usuario(ids_usuario : str, fecha : str):
     # id = id_usuario.replace("hela-","")
     results = conn.actividad_diaria_usuario(ids_usuario,fecha)
     return actividades_diaria_schema(results)

@router.get("/backoffice/usuario")
async def get_backoffice_usuario(ids_usuario : str):
     # id = id_usuario.replace("hela-","")
     results = conn.toc_backoffice_usuario(ids_usuario)
     # ciudad = conn.get_comuna_por_ruta_manual()

     backoffice = backoffices_usuario_schema(results)
     # for result in backoffice:
     #      if result["Comuna"] is None:
     #       print(result["Comuna"] )
     #       ciudad = conn.get_comuna_por_ruta_manual()[0]
     #       result["Comuna"] = ciudad
     return backoffice

@router.get("/tracking")
async def toc_tracking(cod_producto : str):
    results = conn.read_toc_tracking(cod_producto)
    toc_tracking = toc_tracking_schema(results)
    pattern = r'\bportal-\b'

    for usu in toc_tracking:
          if re.search(pattern,usu['Creado_por']) :
               id = usu['Creado_por'].replace("portal-","")
               nombre_usu = connUser.get_nombre_usuario(id)[0]
               usu['Creado_por'] = nombre_usu
          else:
               id_hela = usu['Creado_por'].replace("hela-","")
               nombre_usu_hela = connHela.get_nombre_usuario_hela(id_hela)[0]
               usu['Creado_por'] = nombre_usu_hela

    return toc_tracking

@router.put("/editar")
async def editar_alerta(body : EditarTOC):
     try:
          data = body.dict()
          if body.Alerta == True:
               conn.update_alerta_bitacora_toc_by_guia(body.Guia)

          row = conn.update_subestado_esperado(data)

          if row != 0:
               print("Se registra")
               #   connHela.insert_data_bitacora_recepcion(data)
          else:
               print(" no se actualizaron ")

          return { "message": f"alerta actualizada correctamente",
                    "extra": f"alerta : {body.Alerta}"}
     except:
          print("error en toc/editar")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la actualizacion")


@router.get("/buscar/alerta/{ids_ty}")
async def buscar_alerta(ids_ty : str):
     results = conn.buscar_alerta_by_ids_transyanez(ids_ty)
     print(results)
     return buscar_alertas_schema(results)

@router.get("/guia/{codigo}")
async def get_guia_by_codigo(codigo : str):
     result = conn.read_guia_toc(codigo)
     return {
          "Guia" : result[0]
          }

@router.get("/diferencia/fechas/easy")
async def get_differencia_fechas_easy(fecha_inicio : str,fecha_fin : str, offset: int):
    result = conn.obtener_dif_fechas_easy_excel(fecha_inicio,fecha_fin , offset)
    largo = len(result)
    return {
         "visible": True,
         "items" : largo,
         "datos" : dif_fecha_easy_schema(result)
    }
     


# @router.get("/diferencia/fechas/easy/descargar")
# async def datos_rutas_mes(dia : str):
#     result = conn.get_reportes_rutas_diario(dia)
#     nombre_filas = ( "Fecha Ruta", "Ruta", "Ruta Beetrack", "Posición", "Código Cliente", "Nombre",
#                     "Dirección", "Comuna","Región","Telefono","Email","Código Pedido","Fecha pedido",
#                     "Descripción","Cantidad","Creado por", "Daño embalaje", "Daño producto",
#                     "Pickeado")
#     nombre_excel = f"inventario-rutas-{dia}"

#     return excel.generar_excel_generico(result,nombre_filas,nombre_excel)







