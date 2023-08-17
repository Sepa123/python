from fastapi import APIRouter, status,HTTPException
from typing import List

##Conexiones
from database.client import reportesConnection

##Modelos Schemas

from database.models.toc.toc_bitacora import BitacoraToc

from database.schema.toc.producto_toc import buscar_producto_toc_schema

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

@router.post("/registrar_bitacora")
async def buscar_producto(body : BitacoraToc , status_code=status.HTTP_201_CREATED):
    data = body.dict()
    return data