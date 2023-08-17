from fastapi import APIRouter, status,HTTPException
from typing import List

##Conexiones
from database.client import reportesConnection

##Modelos Schemas

from database.models.toc.toc_bitacora import BitacoraToc
from database.schema.toc.producto_toc import buscar_producto_toc_schema

from database.schema.toc.subestados import subestados_schema
from database.schema.toc.codigo1 import codigos1_schema

router = APIRouter(tags=["TOC"], prefix="/api/toc")

conn = reportesConnection()

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
    try:
        data = body.dict()
        conn.insert_bitacora_toc(data)
        return {"message": "Bitacora tanto registrada Correctamente"}
    except:
        print("error")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error al registrar la bitacora")