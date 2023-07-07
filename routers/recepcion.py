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

## buscar los productos que llegan el dia de hoy

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


## buscar los productos que llegan el dia de hoy por su codigo de producto


@router.put("/electrolux", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_electrolux(body: bodyUpdateVerified):
    try:
        data = body.dict()
        rows = conn.update_verified_electrolux(body.cod_producto)
        # connHela.insert_data_bitacora_recepcion(data)
        return { "message": f"Producto {body.cod_producto} verificado." }
    except:
          print("error")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la verificación")

@router.put("/sportex", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_sportex_by_codigo_producto(body: bodyUpdateVerified):
    try:
        data = body.dict()
        rows = conn.update_verified_sportex(body.cod_producto)
        # connHela.insert_data_bitacora_recepcion(data)
        return { "message": f"Producto {body.cod_producto} verificado." }
    except:
          print("error")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la verificación") 

@router.put("/easy_opl", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_easy_opl_by_codigo_producto(body: bodyUpdateVerified):
    codigo = conn.get_codigo_pedido_opl(body.cod_producto)
    print(codigo)
    body.n_guia = codigo[0][0]
    try:
        data = body.dict()
        rows = conn.update_verified_opl(codigo[0][0])
        # connHela.insert_data_bitacora_recepcion(data)

        return { "message": f"Producto {codigo[0][0]} verificado." }

    except:
          print("error")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la verificación")

@router.put("/easy_cd", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_easy_cd_by_codigo_producto(body: bodyUpdateVerified):
    # results = conn.read_recepcion_easy_cd_by_codigo_producto(body.cod_producto)
    try:
        data = body.dict()
        rows = conn.update_verified_cd(body.cod_producto)
        # connHela.insert_data_bitacora_recepcion(data)
        return { "message": f"Producto {body.cod_producto} verificado." }
    except:
          print("error")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la verificación")



### Actualización de los verificados de los productos
@router.put("/verificar",status_code=status.HTTP_202_ACCEPTED)
async def update_verificado_producto(body: bodyUpdateVerified):
    try:
        data = body.dict()
        print(body.cod_producto)
        rows = conn.update_verified_recepcion(body.cod_pedido,body.cod_producto)
        print(rows)
        if any(number != 0 for number in rows):
            # connHela.insert_data_bitacora_recepcion(data)
            return { "message": f"Producto de codigo {body.cod_producto} verificado." }
        else:
            return { "message": f"Producto ya fue verificado." }
        
    except:
          print("error")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")


@router.get("/prueba/{codigo}")
async def prueba(codigo : str):
    codigo_pedido = conn.get_codigo_pedido_opl(codigo)

    return codigo_pedido[0][0]


@router.put("/verificar/opl",status_code=status.HTTP_202_ACCEPTED)
async def update_verificado_producto(body: bodyUpdateVerified):

    codigo_pedido = conn.get_codigo_pedido_opl(body.cod_pedido)

    return ""


#Bitacora podria ir junto al updateverificar

# @router.post("/bitacora",status_code=status.HTTP_201_CREATED)
