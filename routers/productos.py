from fastapi import APIRouter, status,HTTPException
from fastapi.responses import FileResponse
from openpyxl import Workbook
import re

## modelos y schemas

from database.models.producto_sin_clasificacion import ProductoSinClasificacion
from database.schema.productos_sin_clasificacion import productos_sin_clasificacion_schema

from database.models.producto_picking import producto_picking
from database.schema.producto_picking import productos_picking_schema ,producto_picking_schema

from database.client import reportesConnection
from fastapi.responses import FileResponse

router = APIRouter(tags=["productos"], prefix="/api/productos")

conn = reportesConnection()

@router.get("/sin_clasificacion")
async def get_productos_sin_clasificar():
    results = conn.read_productos_sin_clasificar()
    if not results:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")
    
    return productos_sin_clasificacion_schema(results)


@router.post("/sin_clasificacion", status_code=status.HTTP_201_CREATED)
async def insert_producto_sin_clasificar(producto : ProductoSinClasificacion):
        # try:
            
            data =  producto.dict()
            print(data)
            conn.write_producto_sin_clasificar(data)
            return { "message":"Producto agregado correctamente" }
            # conn.write_producto_sin_clasificar(data)
        # except:
        #     print("error")
        #     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")

#producto picking

@router.get("/buscar/producto")
async def productos_picking():
    results = conn.get_producto_picking()
    return producto_picking_schema(results)

@router.get("/buscar/producto/{producto_id}",status_code=status.HTTP_202_ACCEPTED)
async def producto_picking_id(producto_id : str):
    results = conn.get_producto_picking_id(producto_id)
    # print(results[""])
    print("/buscar/producto/")
    if results is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="El codigo del producto no existe")
    return producto_picking_schema(results)

