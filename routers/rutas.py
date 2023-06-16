from fastapi import APIRouter, status,HTTPException
from fastapi.responses import FileResponse
from openpyxl import Workbook
from openpyxl.styles import Font

import re
import json
from typing import List

## conexion

from database.client import reportesConnection

## modelos y schemas

from database.models.ruta_manual import RutaManual
from database.schema.ruta_manual import convert_to_json
from database.models.ruta_en_activo import RutaEnActivo
from database.schema.rutas_en_activo import rutas_en_activo_schema
from database.schema.nombres_rutas_activas import nombres_rutas_activas_schema

from database.schema.datos_ruta_activa_editar import datos_rutas_activas_editar_schema

router = APIRouter(tags=["rutas"], prefix="/api/rutas")

conn = reportesConnection()

@router.get("/buscar/{pedido_id}",status_code=status.HTTP_202_ACCEPTED)
async def get_ruta_manual(pedido_id : str):
    results = conn.get_ruta_manual(pedido_id)

    check = conn.check_producto_existe(pedido_id)
    check = re.sub(r'\(|\)', '',check[0])
    check = check.split(",")

    print(check)
    
    if(check[0] == "1"):
        print("codigo pedido repetido")
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED, 
                            detail=f"El Producto {pedido_id} se encuentra en la ruta {check[1]}")
    
    if results is None or results == []:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="El codigo del producto no existe")
    
    json_data = convert_to_json(results)
    # print(results)
    print("/buscar/ruta")

    return json_data


@router.post("/agregar",status_code=status.HTTP_201_CREATED)
async def insert_ruta_manual(rutas : List[List[RutaManual]]):
    # try:
        print(len(rutas))
        id_ruta = conn.read_id_ruta()[0]
        nombre_ruta = conn.get_nombre_ruta_manual(rutas[0][0].Created_by)[0][0]

        check = conn.check_producto_existe(rutas[0][0].Codigo_pedido)
        check = re.sub(r'\(|\)', '',check[0])
        check = check.split(",")

        print(check)

        if(check[0] == "1"):
            print("codigo pedido repetido")
            raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED, 
                                detail=f"El Producto {rutas[0][0].Codigo_pedido} se encuentra en la ruta {check[1]}")
        for ruta in rutas:
            for producto in ruta:
                data = producto.dict()
                data["Id_ruta"] = id_ruta
                data["Agrupador"] = nombre_ruta
                data["Nombre_ruta"] = nombre_ruta
                data["Pistoleado"] = True 
                # conn.update_verified(data["Codigo_producto"])
                conn.write_rutas_manual(data)
        return { "message": f"La Ruta {nombre_ruta} fue guardada exitosamente" }
    # except:
    #     print("error")
    #     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")

@router.put("/actualizar/estado/{cod_producto}",status_code=status.HTTP_202_ACCEPTED)
async def update_estado_producto(cod_producto:str):
     try:
          print(cod_producto)
          conn.update_verified(cod_producto)
          return { "message": "Producto Actualizado Correctamente" }
     except:
          print("error")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")
     
@router.get("/listar/activo",status_code=status.HTTP_200_OK)
async def get_rutas_en_activo(nombre_ruta : str):
     print(nombre_ruta)
     results = conn.read_rutas_en_activo(nombre_ruta)

     if results is None or results == []:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="la ruta no existe")
     
     return rutas_en_activo_schema(results)

# rutas activas

# @router.post("/agregar/ruta_activa",status_code=status.HTTP_201_CREATED)
# async def insert_ruta_manual(rutas : List[List[RutaManual]]):
#     # try:
#         print(len(rutas))
#         id_ruta = conn.read_id_ruta()[0]
#         nombre_ruta = conn.get_nombre_ruta_manual(rutas[0][0].Created_by)[0][0]

#         check = conn.check_producto_existe(rutas[0][0].Codigo_pedido)
#         check = re.sub(r'\(|\)', '',check[0])
#         check = check.split(",")

#         print(check)

#         if(check[0] == "1"):
#             print("codigo pedido repetido")
#             raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED, 
#                                 detail=f"El Producto {rutas[0][0].Codigo_pedido} se encuentra en la ruta {check[1]}")
#         for ruta in rutas:
#             for producto in ruta:
#                 data = producto.dict()
#                 data["Id_ruta"] = id_ruta
#                 data["Agrupador"] = nombre_ruta
#                 data["Nombre_ruta"] = nombre_ruta
#                 data["Pistoleado"] = True 
#                 # conn.update_verified(data["Codigo_producto"])
#                 conn.write_rutas_manual(data)
#         return { "message": f"La Ruta {nombre_ruta} fue guardada exitosamente" }
#     # except:
#     #     print("error")
#     #     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")

  
@router.get("/activo/nombre_ruta")
async def get_nombres_ruta(fecha : str):
    results = conn.read_nombres_rutas(fecha)
    return nombres_rutas_activas_schema(results)


@router.put("/actualizar/estado/activo/{nombre_ruta}",status_code=status.HTTP_202_ACCEPTED)
async def update_estado_ruta(nombre_ruta:str):
     try:
          print(nombre_ruta)
          conn.update_estado_rutas(nombre_ruta)
          return { "message": "Estado de Ruta Actualizado Correctamente" }
     except:
          print("error")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")

@router.get("/datos_ruta/{nombre_ruta}",status_code=status.HTTP_202_ACCEPTED)
async def get_ruta_by_nombre_ruta(nombre_ruta: str):
     print(nombre_ruta)
     results = conn.read_ruta_activa_by_nombre_ruta(nombre_ruta)
     return datos_rutas_activas_editar_schema(results)

@router.delete("/eliminar/producto/{cod_producto}") 
async def delete_producto_ruta_activa(cod_producto : str):
     try:
          results = conn.delete_producto_ruta_activa(cod_producto)
          if (results == 0): return { "message" : "El producto no existe en ninguna ruta"}
          return { "message" : "producto eliminado"}
     except:
          print("error")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")
       
# @router.get("/descargar")
# async def download_excel(nombre_ruta : str):
#     datos = [[]]
  
#     datos.append([
#         "Posición", "Pedido", "Comuna", "SKU", "Producto", "UND", "Bultos", "Nombre",
#         "Direccion Cliente", "Teléfono", "Validado", "DE", "DP"
#     ])
  
#     ruta_en_activo = conn.read_rutas_en_activo(nombre_ruta) # Ruta en activo (tu implementación aquí)
    
#     # Crear un libro de Excel y seleccionar la hoja activa
#     libro_excel = Workbook()
#     hoja = libro_excel.active
#     hoja.title = 'Hoja1'
  
#     # Estilo para el texto en negrita
#     negrita = Font(bold=True)
  
#     # for ruta in ruta_en_activo:
#     #     if len(ruta.arrayProductos) == 1:
#     #         fila = [
#     #             ruta.Pos, ruta.Codigo_pedido, ruta.Comuna, ruta.SKU, ruta.Producto,
#     #             ruta.Unidades, ruta.Bultos, ruta.Nombre_cliente, ruta.Direccion_cliente, ruta.Telefono
#     #         ]
#     #         datos.append(fila)
#     #     elif len(ruta.arrayProductos) > 1:
#     #         for i, producto in enumerate(ruta.arrayProductos):
#     #             if i == 0:
#     #                 fila = [
#     #                     ruta.Pos, ruta.Codigo_pedido, ruta.Comuna, ruta.arraySKU[i], producto,
#     #                     ruta.Unidades, ruta.Bultos, ruta.Nombre_cliente, ruta.Direccion_cliente, ruta.Telefono
#     #                 ]
#     #                 datos.append(fila)
#     #             else:
#     #                 fila_producto = [
#     #                     "", "", "", ruta.arraySKU[i], producto,
#     #                     "", "", "", "", ""
#     #                 ]
#     #                 datos.append(fila_producto)
  
#     # Escribir los datos en la hoja
#     for fila in datos:
#         hoja.append(fila)
  
#     # Aplicar estilo en negrita a la primera fila
#     for celda in hoja[2]:
#         celda.font = negrita
  
#     # Guardar el archivo
#     nombre_archivo = f"{nombre_ruta}.xlsx"
#     libro_excel.save(nombre_archivo)


#     return FileResponse(f"{nombre_archivo}")