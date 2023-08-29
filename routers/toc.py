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
          print("error")
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

        id_transyanez = conn.id_transyanez_bitacora()[0]
        body.Id_transyanez = id_transyanez
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
     return backoffices_usuario_schema(results)





