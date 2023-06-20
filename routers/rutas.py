from fastapi import APIRouter, status,HTTPException
from fastapi.responses import FileResponse
from openpyxl import Workbook
from openpyxl.styles import Font , PatternFill, Border ,Side

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

@router.post("/agregar/ruta_activa_existente",status_code=status.HTTP_201_CREATED)
async def insert_ruta_existente_activa(rutas : List[List[RutaManual]]):
    try:
        print(len(rutas))
        id_ruta = rutas[0][0].Id_ruta
        nombre_ruta = rutas[0][0].Nombre_ruta

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
        return { "message": f"La Ruta {nombre_ruta} fue actualizada exitosamente" }
    except:
        print("error")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")

  
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
       
@router.get("/descargar")
async def download_excel(nombre_ruta : str):
    datos = [[]]
  
    datos.append([
        "Posición", "Pedido", "Comuna","Producto","SKU", "UND", "Bultos", "Nombre",
        "Direccion Cliente", "Teléfono", "Validado", "DE", "DP"
    ])
  
    result = conn.read_rutas_en_activo(nombre_ruta) 
    

    border = Border(left=Side(border_style='thin', color='000000'),   
                right=Side(border_style='thin', color='000000'),   
                top=Side(border_style='thin', color='000000'),     
                bottom=Side(border_style='thin', color='000000')) 
    
    rutas_activas = rutas_en_activo_schema(result)
    # Crear un libro de Excel y seleccionar la hoja activa
    libro_excel = Workbook()
    hoja = libro_excel.active
    hoja.title = 'Hoja1'
  
    # Estilo para el texto en negrita
    negrita = Font(bold=True, size=20,  color='000000')
    # hoja.merge_cells('A1:D1')
    hoja.append(("Ruta : "+nombre_ruta,))

    for ruta in rutas_activas:
        arrayProductos = ruta["Producto"].split("@")
        arraySKU = ruta["SKU"].split("@")
        if len(arrayProductos) == 1:
            fila = [
                ruta["Pos"], ruta["Codigo_pedido"], ruta["Comuna"], arrayProductos[0], arraySKU[0],
                ruta["Unidades"], ruta["Bultos"], ruta["Nombre_cliente"], ruta["Direccion_cliente"], ruta["Telefono"]
            ]
            datos.append(fila)
        elif len(arrayProductos) > 1:
            for i, producto in enumerate(arrayProductos):
                if i == 0:
                    fila = [
                        ruta["Pos"], ruta["Codigo_pedido"], ruta["Comuna"], producto, arraySKU[0],
                        ruta["Unidades"], ruta["Bultos"], ruta["Nombre_cliente"], ruta["Direccion_cliente"], ruta["Telefono"]
                    ]
                    datos.append(fila)
                else:
                    fila_producto = [
                        "", "", "",producto, arraySKU[i],
                        "", "", "", "", ""
                    ]
                    datos.append(fila_producto)
  
    # Escribir los datos en la hoja
    for i,fila in enumerate(datos):
        hoja.append(fila)
        nHoja = i
        
    # Aplicar estilo en negrita a la primera fila
    for celda in hoja[1]:
        celda.font = negrita

    for celda in hoja[3]:
        celda.font = Font(bold=True, color="FFFFFF")
        celda.fill = PatternFill(start_color="000000FF", end_color="000000FF", fill_type="solid")
        celda.border = border
    
    # print(nHoja)
    for n in range(nHoja):
         for celda in hoja[n+4]:
             celda.font = Font(bold=True, color="070707")
             celda.fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
             celda.border = border
         
    # Guardar el archivo
    nombre_archivo = "nombre_ruta.xlsx"
    libro_excel.save(nombre_archivo)


    return FileResponse("nombre_ruta.xlsx")