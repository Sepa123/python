from datetime import datetime
from fastapi import APIRouter, status,HTTPException
##Modelos y schemas

from database.schema.rsv.catalogo_producto import catalogos_productos_schema
from database.models.rsv.catalogo_producto import CatalogoProducto

from database.schema.rsv.colores import colores_rsv_schema

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


@router.get("/colores")
async def obtener_colores_rsv():
    result = conn.read_colores_rsv()
    return colores_rsv_schema(result)


@router.get("/buscar/{codigo}")
async def buscar_producto_existente(codigo : str):
    result = conn.buscar_producto_existente_rsv(codigo)
    if result is None:
        return { "repetido": False}
    else:
        return { 
            "repetido": True,
            "message": f"El codigo {result[0]} ya existe"}


@router.post("/agregar/producto")
async def agregar_nuevo_catalogo_rsv(body : CatalogoProducto):
    data = body.dict()
    conn.insert_nuevo_catalogo_rsv(data)
    return {
        "message": "Producto agregado correctamente"
    }


@router.delete("eliminar/catalogo/{catalogo}")
async def agregar_nuevo_catalogo_rsv(catalogo):
    return ""