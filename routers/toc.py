from fastapi import APIRouter, status,HTTPException, UploadFile, File
from typing import List

##Conexiones
from database.client import reportesConnection

##Modelos Schemas

from database.schema.toc.producto_toc import buscar_producto_toc_schema

router = APIRouter(tags=["TOC"], prefix="/api/toc")

conn = reportesConnection()

@router.get("/buscar_producto/{cod_producto}")
async def buscar_producto(cod_producto : str):
    result = conn.buscar_producto_toc(cod_producto)
    return buscar_producto_toc_schema(result)