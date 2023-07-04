from fastapi import APIRouter, status,HTTPException
from fastapi.responses import FileResponse
from openpyxl import Workbook

## Modelos y schemas
from database.models.recepcion.recepcion_tiendas import bodyUpdateVerified, Recepcion_tiendas
from database.schema.recepcion.recepcion_tiendas import recepcion_tiendas_schema

##Conexiones

from database.client import reportesConnection
from database.hela_prod import HelaConnection


router = APIRouter(tags=["recepcion"], prefix="/api/recepcion")

conn = reportesConnection()

connHela = HelaConnection()

@router.get("/electrolux", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_electrolux():
    results = conn.read_recepcion_electrolux()

    return recepcion_tiendas_schema(results)

@router.get("/sportex", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_sportex():
    results = conn.read_recepcion_sportex()

    return recepcion_tiendas_schema(results)

@router.get("/easy_opl", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_easy_opl():
    results = conn.read_recepcion_easy_opl()

    return recepcion_tiendas_schema(results)

@router.get("/easy_cd", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_easy_cd():
    results = conn.read_recepcion_easy_cd()

    return recepcion_tiendas_schema(results)

#### No estoy seguro si deba actualizar esto
@router.put("/verificar",status_code=status.HTTP_202_ACCEPTED)
async def update_verificado_producto(body: bodyUpdateVerified):
    try:
        print(body.cod_producto)
        # conn.update_producto_picking_OPL(body.cod_producto,body.cod_sku)
        return { "message": f"Producto de codigo {body.cod_producto} verificado, na mentira" }
    except:
          print("error")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")