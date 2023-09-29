from fastapi import APIRouter, status,HTTPException
##Modelos y schemas
from typing import List
from database.schema.rsv.catalogo_producto import catalogos_productos_schema , codigos_por_color_schema
from database.models.rsv.catalogo_producto import CatalogoProducto

from database.schema.rsv.etiquetas import etiquetas_productos_schema , datos_productos_etiquetas_schema

from database.schema.rsv.colores import colores_rsv_schema
from database.models.rsv.carga_rsv import CargaRSV

from database.models.rsv.lista_eliminar import ListaEliminar

from database.schema.rsv.sucursales import sucursales_rsv_schema

from database.schema.rsv.inventario_sucursal import inventarios_sucursal_schema

from database.schema.rsv.obtener_etiqueta_carga import obtener_etiquetas_carga_schema

from database.schema.rsv.cargas_rsv import cargas_rsv_schema , lista_cargas_schema

from database.schema.rsv.tipo_despacho import tipos_despacho_schema

from database.schema.rsv.datos_carga_etiqueta import datos_cargas_etiquetas_schema

from database.models.rsv.nota_venta import NotaVenta
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


## buscar catalogo por color
@router.get("/catalogo/color")
async def obtener_catalogo_rsv(color : int):
    result = conn.read_catalogo_by_color_rsv(color)
    return codigos_por_color_schema(result)


@router.get("/catalogo/color/sin_filtro")
async def obtener_catalogo_sin_filtro_rsv():
    result = conn.read_catalogo_by_color_sin_filtro_rsv()
    return codigos_por_color_schema(result)


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


@router.put("/editar/producto")
async def editar_nuevo_catalogo_rsv(body : CatalogoProducto):
    body.Codigo_Original = body.Codigo
    print(body)
    data = body.dict()
    conn.update_catalogo_rsv(data)
    print(data)
    return {
        "message": "Producto editado correctamente"
    }

@router.delete("eliminar/catalogo/{catalogo}")
async def agregar_nuevo_catalogo_rsv(catalogo):
    return ""


@router.get("/cargas")
async def obtener_carga_rsv():
    result = conn.read_cargas_rsv()
    return cargas_rsv_schema(result)


@router.get("/cargas/nombre_carga/{nombre_carga}")
async def obtener_carga_rsv(nombre_carga : str):
    result = conn.read_cargas_por_nombre_carga_rsv(nombre_carga)
    return cargas_rsv_schema(result)

@router.get("/datos/etiquetas/carga/{nombre_carga}")
async def obtener_datos_carga_rsv(nombre_carga : str):
    result = conn.read_datos_carga_por_nombre_rsv(nombre_carga)
    return datos_cargas_etiquetas_schema(result)

@router.get("/listar/cargas")
async def obtener_carga_rsv():
    result = conn.read_lista_carga_rsv()
    return lista_cargas_schema(result)

@router.get("/listar/cargas/mes")
async def obtener_carga_rsv_por_mes(mes : str):
    result = conn.read_lista_carga_rsv_por_mes(mes)
    return lista_cargas_schema(result)


@router.post("/agregar/carga")
async def insert_carga_rsv(list_body : List[CargaRSV]):
    try:
        if list_body == []:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se ha agregado ningun producto a a la carga")
        print(list_body)
        for body in list_body:
            data = body.dict()
            nombre_carga = body.Nombre_carga
            print(data)
            conn.insert_carga_rsv(data)
        # print(data)
        return {
            "message": f"{nombre_carga} agregada correctamente"
        }
    except:
        print(" No, pase aca")
        if list_body == []:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se ha agregado ningun producto a a la carga")
        else:

            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error al ingresar la carga")

## buscar si nombre_carga existe
@router.get("/carga/buscar")
async def buscar_carga_por_nombre(nombre_carga : str):
    result = conn.buscar_cargas_rsv(nombre_carga)
    print("resultado", result)
    if result is None:
        return { "repetido": False}
    else:
        return { 
            "repetido": True,
            "message": f"La carga {result[0]} ya existe"}
    
@router.get("/etiquetas")
async def get_etiquetas_rsv():
    result = conn.read_etiquetas_rsv()
    return etiquetas_productos_schema(result)


@router.get("/etiquetas/carga/descargar")
async def download_etiquetas_carga(nombre_carga : str, codigo: str,tipo : str):

    results = conn.obtener_etiqueta_carga_rsv(nombre_carga,codigo,tipo)
    wb = Workbook()
    ws = wb.active
    results.insert(0, ("bar_code","codigo_imp","descripcion","color"))

    for row in results:
        ws.append(row)
    
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter # get column letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column].width = adjusted_width
    
    wb.save("excel/etiquetas_carga.xlsx")

    return FileResponse("excel/etiquetas_carga.xlsx")


@router.get("/etiquetas/carga")
async def get_etiquetas_carga(nombre_carga : str, codigo: str):
    results = conn.obtener_etiqueta_carga_rsv(nombre_carga,codigo)
    return obtener_etiquetas_carga_schema(results)



@router.get("/generar/etiquetas")
async def generar_etiquetas_por_nombre_carga(nombre_carga :str):
    results = conn.generar_etiquitas_rsv(nombre_carga)
    return {
        "alerta": results[0][0],
        "message": results[0][1]
    }

@router.get("/datos/etiquetas/productos")
async def get_datos_productos_etiquetas(nombre_carga : str):
    results = conn.read_datos_productos_etiquetas_rsv(nombre_carga)
    return datos_productos_etiquetas_schema(results)


@router.get("/sucursales")
async def get_sucursales_rsv():
    results = conn.read_sucursales_rsv()
    return sucursales_rsv_schema(results)

@router.get("/inventario/sucursales/{sucursal}")
async def get_inventario_por_sucursal(sucursal : int):
    results = conn.obtener_inventario_por_sucursal(sucursal)
    return inventarios_sucursal_schema(results)

## eliminar cargas
@router.put("/eliminar/cargas")
async def update_carga(lista : ListaEliminar):

    if lista.lista == '':
        return {
        "message" : "no hay nada que eliminar"
    }
    codigos = lista.lista.split(',')

    print(codigos)

    print(', '.join(['%s']*len(codigos)))
    print(lista.nombre_carga)

    results = conn.delete_cargas(lista.lista, lista.nombre_carga)
    return {
        "message" : f"Cargas eliminadas ,{results}"
    }

@router.put("/editar/carga")
async def update_carga(list_body : List[CargaRSV]):
    try:
        if list_body == []:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se ha agregado ningun producto a a la carga")
        print(list_body)
        for body in list_body:
            data = body.dict()
            nombre_carga = body.Nombre_carga
            print(data)
            #verificar si producto existe en esta carga
            check = conn.check_codigo_existente_carga(nombre_carga, body.Codigo)
            print(check)
            # update de codigo existente
            if len(check) != 0:
                conn.update_carga_rsv(data)
                print("editado")
            else:
                print("insertar")
                conn.insert_carga_rsv(data)
            
        # print(data)
        return {
            "message": f"{nombre_carga} actualizada correctamente"
        }
    except:
        print(" No, pase aca")
        if list_body == []:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No se ha agregado ningun producto a a la carga")
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error al ingresar la carga")

@router.get("/tipo/despacho")
async def get_tipo_despacho():
    results = conn.read_tipo_despacho_rsv()
    return tipos_despacho_schema(results)

# NotaVenta

@router.post("/agregar/nota_venta")
async def insert_nota_venta(body : NotaVenta):
    print(body)
    return "tipos_despacho_schema(results)"