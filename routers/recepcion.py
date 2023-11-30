from fastapi import APIRouter, status,HTTPException
from fastapi.responses import FileResponse
from openpyxl import Workbook

from lib.obtener_dia_anterior import obtener_dia_anterior

## Modelos y schemas
from database.models.recepcion.recepcion_tiendas import bodyUpdateVerified, Recepcion_tiendas
from database.schema.recepcion.recepcion_tiendas import recepcion_tiendas_schema, recepcion_easy_cds_schema
from database.schema.recepcion.recepcion_pendiente import recepcion_pendiente_schema
##Conexiones

from database.client import reportesConnection
from database.hela_prod import HelaConnection


router = APIRouter(tags=["recepcion"], prefix="/api/recepcion")

conn = reportesConnection()

connHela = HelaConnection()

## buscar los productos que llegan el dia de hoy

##productos sin recepcion todos los clientes

@router.get("/producto_sin_recepcion" , status_code=status.HTTP_202_ACCEPTED)
async def get_productos_no_recepcionados():
    results = conn.read_productos_sin_recepcion()
    return recepcion_pendiente_schema(results)

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
    # return recepcion_tiendas_schema(results)
    return recepcion_easy_cds_schema(results)

@router.get("/easy_cd", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_easy_cd():
    dia_anterior = obtener_dia_anterior()
    results = conn.read_recepcion_easy_cd(dia_anterior)
    return recepcion_easy_cds_schema(results)


## buscar los productos que llegan el dia de hoy por su codigo de producto

@router.get("/electrolux/{cod_pedido}", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_electrolux(cod_pedido : str):
    results = conn.read_recepcion_electrolux_by_codigo_producto(cod_pedido)

    if len(results) == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El  código {cod_pedido} no se pudo encontrar")

    return recepcion_tiendas_schema(results)

@router.get("/easy_opl/{cod_pedido}", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_easy_opl_by_codigo_pedido(cod_pedido : str):
    codigo_pedido = conn.get_codigo_pedido_opl(cod_pedido)
    results = conn.read_recepcion_easy_opl_by_codigo_producto(codigo_pedido[0][0])

    if len(results) == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El  código {codigo_pedido[0][0]} no se pudo encontrar")

    return recepcion_tiendas_schema(results)

## cambiar verificado a productos 

@router.put("/electrolux", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_electrolux(body: bodyUpdateVerified):
    try:

        print(f" producto electrolux verificar {body.cod_producto}")
        data = body.dict()
        rows = conn.update_verified_electrolux(body.cod_producto)
        if rows != 0:
            connHela.insert_data_bitacora_recepcion(data)
        else:
            print(" no se verifico ningun producto")
        return { "message": f"Producto {rows} verificado." }
    except:
          print("error con verificar electrolux")
          if rows == 0:
               raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El  producto {body.cod_producto} no se pudo verificar")
          
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la verificación")

@router.put("/sportex", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_sportex_by_codigo_producto(body: bodyUpdateVerified):
    try:
        data = body.dict()
        rows = conn.update_verified_sportex(body.cod_producto)
        if rows != 0:
            connHela.insert_data_bitacora_recepcion(data)
        else:
            print(" no se verifico producto en sportex")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El  producto {body.cod_producto} no se pudo verificar")

        return { "message": f"Producto {rows} verificado." }
    except:
          print("error con verificar sportex")
          if rows == 0:
               raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El  producto {body.cod_producto} no se pudo verificar")
          
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la verificación") 

@router.put("/easy_opl", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_easy_opl_by_codigo_producto(body: bodyUpdateVerified):
    codigo = conn.get_codigo_pedido_opl(body.cod_producto)
    print(codigo)
    print(f"producto El  producto {body.cod_producto} OPL, codigo[0][0] = {codigo[0][0]}")
    body.n_guia = codigo[0][0]
    try:
        data = body.dict()
        rows = conn.update_verified_opl(codigo[0][0], body.sku)
        if rows != 0:
            connHela.insert_data_bitacora_recepcion(data)
        else:
            print(" no se verifico ningun producto")

        return { "message": f"Producto {rows} verificado {body.sku}." }

    except:
          print("error con verificar easy OPL")
          if rows == 0:
               raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El  producto {body.cod_producto} no se pudo verificar")
          
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la verificación")

@router.put("/easy_cd", status_code=status.HTTP_202_ACCEPTED)
async def get_recepcion_easy_cd_by_codigo_producto(body: bodyUpdateVerified):
    # results = conn.read_recepcion_easy_cd_by_codigo_producto(body.cod_producto)
    try:
        data = body.dict()
        print(f"El  producto {body.cod_producto} CD")
        
        rows = conn.update_verified_cd(body.cod_producto)
        print(rows)
        if rows != 0:
            connHela.insert_data_bitacora_recepcion(data)
        else:
            print(" no se verifico ningun producto")
        return { "message": f"Producto {rows} verificado." }
    except:
          print("error con verificar EASY CD")
          if rows == 0:
               raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El  producto {body.cod_producto} no se pudo verificar")
          
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la verificación")



### Actualización de los verificados de los productos
@router.put("/verificar",status_code=status.HTTP_202_ACCEPTED)
async def update_verificado_producto(body: bodyUpdateVerified):
    try:
        data = body.dict()
        print(body.cod_producto)
        rows = conn.update_verified_recepcion(body.cod_pedido,body.cod_producto,body.sku)
        print(rows)
        if any(number != 0 for number in rows):
            connHela.insert_data_bitacora_recepcion(data)
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


@router.put("/easy_cd/actualizar")
async def update_recepcion_easy_cd_by_codigo_producto(body: bodyUpdateVerified):
    # results = conn.read_recepcion_easy_cd_by_codigo_producto(body.cod_producto)
    try:
        data = body.dict()        
        rows = conn.update_recepcion_cd(body.cod_producto)
        print(f"easy_cd codigo {body.cod_producto}")
        print(rows)
        if rows != 0:
            connHela.insert_data_bitacora_recepcion(data)
        else:
            print(" no se verifico ningun producto")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El  producto {body.cod_producto} no se pudo verificar")

        return { "message": f"Producto {rows} verificado." }
    except:
          print("error al actualizar cd")
          if rows == 0:
               raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El  producto {body.cod_producto} no se pudo verificar")
          
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la verificación")

@router.put("/easy_opl/actualizar")
async def update_recepcion_easy_opl_by_codigo_producto_sko(body: bodyUpdateVerified):
    # results = conn.read_recepcion_easy_cd_by_codigo_producto(body.cod_producto)
    # try:
        # print(body.cod_pedido)
        codigo_pedido = conn.get_codigo_pedido_opl(body.cod_pedido)[0][0]
        # print(codigo_pedido)
        body.n_guia = codigo_pedido
        body.cod_producto = codigo_pedido
        data = body.dict()     
        rows = conn.update_recepcion_opl(codigo_pedido, body.sku)
        print(f"easy_opl codigo {codigo_pedido} y sku {body.sku}")
        print(rows)
        if rows != 0:
            connHela.insert_data_bitacora_recepcion(data)
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"El  producto {body.cod_producto} no se pudo verificar")
          
            
        return { "message": f"Producto recepcionados : {rows}." }
    # except:
    #       print("error")
    #       raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la verificación")
#Bitacora podria ir junto al updateverificar

# @router.post("/bitacora",status_code=status.HTTP_201_CREATED)
