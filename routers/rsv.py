from datetime import datetime
from fastapi import APIRouter, status,HTTPException
##Modelos y schemas

from database.schema.rsv.catalogo_producto import catalogos_productos_schema

##Conexiones
from database.client import reportesConnection

## en caso de generar un excel
from fastapi.responses import FileResponse
from openpyxl import Workbook
from openpyxl.styles import Font , PatternFill, Border ,Side

router = APIRouter(tags=["RSV"], prefix="/api/rsv")

conn = reportesConnection()

@router.get("/catalogo")
async def obtener_catalogo_rsv():
    result = conn.read_catalogo_rsv()
    return catalogos_productos_schema(result)


@router.post("agregar/catalogo")
async def agregar_nuevo_catalogo_rsv():
    return ""


@router.delete("eliminar/catalogo/{catalogo}")
async def agregar_nuevo_catalogo_rsv(catalogo):
    return ""