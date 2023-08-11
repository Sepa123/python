from fastapi import APIRouter, status,HTTPException
from fastapi.responses import FileResponse
from openpyxl import Workbook
from openpyxl.styles import Font , PatternFill, Border ,Side
from datetime import datetime, timedelta

import time
import re
import json
from typing import List

from geopy.geocoders import Nominatim

## conexion

from database.client import reportesConnection
from database.hela_prod import HelaConnection

## modelos y schemas
from database.models.asignar_ruta import RutasAsignadas
from database.models.recepcion.recepcion_tiendas import bodyUpdateVerified
from database.models.ruta_manual import RutaManual
from database.schema.ruta_manual import convert_to_json, rutas_manuales_schema
from database.models.ruta_en_activo import RutaEnActivo
from database.schema.rutas_en_activo import rutas_en_activo_schema
from database.schema.nombres_rutas_activas import nombres_rutas_activas_schema

from database.schema.datos_ruta_activa_editar import datos_rutas_activas_editar_schema
from database.schema.rutas.driver_ruta_asignada import driver_ruta_asignada

from database.schema.rutas.recuperar_tracking_beetrack import recuperar_tracking_beetrack_schema, recuperar_fecha_ingreso_sistema_schema

from database.schema.rutas.linea_producto import recuperar_linea_producto_schema

from database.schema.geolocalizacion.latlng import latlng_schema
from database.models.geolocalizacion.latlong import Latlong

from database.schema.rutas.archivo_descarga_beetrack import datos_descarga_beetracks_schema
router = APIRouter(tags=["rutas"], prefix="/api/rutas")

conn = reportesConnection()

connHela = HelaConnection()

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
                            detail=f'El Producto "{pedido_id}" se encuentra en la ruta: {check[1]}' )
    
    if results is None or results == []:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="El codigo del producto no existe")
    
    json_data = rutas_manuales_schema(results)

    if json_data[0]['Calle'] is None:
        print("La direccion es null")
        json_data[0]['Calle'] = json_data[0]['Direccion_textual']
    # print(results)
    print("/buscar/ruta")

    return json_data



def validar_fecha(fecha):
    fecha_actual = datetime.now().date()  # Obtiene la fecha actual
    
    # Convierte la fecha dada a un objeto de tipo datetime
    try:
        fecha_ingresada = datetime.strptime(fecha, '%Y-%m-%d').date()
    except ValueError:
        return False  # La fecha no tiene el formato correcto
    
    # Compara las fechas
    if fecha_ingresada >= fecha_actual:
        return True
    else:
        return False

@router.post("/agregar",status_code=status.HTTP_201_CREATED)
async def insert_ruta_manual(rutas : List[List[RutaManual]], fecha_pedido : str):
    try:
        # print(len(rutas))
        if validar_fecha(fecha_pedido) == False: raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED, 
                            detail=f"No se puede crear ruta con la fecha {fecha_pedido}")

        print(fecha_pedido)
        id_ruta = conn.read_id_ruta()[0]
        nombre_ruta = conn.get_nombre_ruta_manual(rutas[0][0].Created_by)[0][0]

        check = conn.check_producto_existe(rutas[0][0].Codigo_pedido)
        check = re.sub(r'\(|\)', '',check[0])
        check = check.split(",")

        # print(check)

        if(check[0] == "1"):
            print("codigo pedido repetido")
            raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED, 
                                detail=f'El Producto "{rutas[0][0].Codigo_pedido}" se encuentra en la ruta {check[1]}')
        for i, ruta in enumerate(rutas):
            for producto in ruta:
                data = producto.dict()
                print(data)
                # data["Calle"] = conn.direccion_textual(data["Codigo_pedido"])[0][0]
                data["Id_ruta"] = id_ruta
                data["Agrupador"] = nombre_ruta
                data["Nombre_ruta"] = nombre_ruta
                data["Pistoleado"] = True 
                print('Nombre', producto.Nombre)
                data["Posicion"] = i + 1
                data["Fecha_ruta"] = fecha_pedido
                if data["Fecha_ruta"] is None:
                    # Obtener la fecha actual
                    fecha_actual = datetime.now().date()
                    # Obtener la fecha del día siguiente
                    fecha_siguiente = fecha_actual + timedelta(days=1)
                    data["Fecha_ruta"] =fecha_siguiente
                conn.write_rutas_manual(data)
        return { "message": f"La Ruta {nombre_ruta} fue guardada exitosamente" }
    except:
        print("error")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")

@router.put("/actualizar/estado/{cod_producto}",status_code=status.HTTP_202_ACCEPTED)
async def update_estado_producto(cod_producto:str, body : bodyUpdateVerified ):
     try:
          data = body.dict()
          conn.update_verified(cod_producto)
          connHela.insert_data_bitacora_recepcion(data)
          return { "message": "Producto actualizado correctamente" }
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
async def insert_ruta_existente_activa(fecha_ruta_nueva : str, rutas : List[List[RutaManual]]):
    try:
        print(len(rutas))
        id_ruta = rutas[0][0].Id_ruta
        nombre_ruta = rutas[0][0].Nombre_ruta

        fecha_ruta = fecha_ruta_nueva

        # print(rutas)

        for i,ruta in enumerate(rutas):
            for producto in ruta:
                data = producto.dict()

                # direccion_textual = conn.direccion_textual(data["Codigo_pedido"])

                # print(data)
                check = conn.check_producto_codigo_repetido(nombre_ruta,data["Codigo_pedido"],data["Codigo_producto"], data["SKU"])
                
                if check is not None:
                    print(data["Codigo_pedido"])
                     
                    print(data["Posicion"])

                    count = conn.update_posicion(data["Posicion"], data["Codigo_pedido"], data["Codigo_producto"], fecha_ruta)
                    print(count)
                else :
                    # data["Calle"] = conn.direccion_textual(data["Codigo_pedido"])[0][0]
                    data["Id_ruta"] = id_ruta
                    data["Agrupador"] = nombre_ruta
                    data["Nombre_ruta"] = nombre_ruta
                    data["Pistoleado"] = True 
                    data["Fecha_ruta"] = fecha_ruta
                    # conn.update_verified(data["Codigo_producto"])
                    print(data)
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
          if (results == 0): print("El producto no existe en ninguna ruta")
          return { "message" : "producto eliminado"}
     except:
          print("error")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")
       
@router.post("/descargar")
async def download_excel(nombre_ruta : str,patente: str,driver:str , body : list):

    datos = [[]]
    
    datos.append([
        "Posición", "Pedido", "Comuna","Producto","SKU", "UND", "Bultos", "Nombre",
        "Direccion Cliente", "Teléfono", "DE", "DP"
    ])
  
    # result = conn.read_rutas_en_activo(nombre_ruta) 

    border = Border(left=Side(border_style='thin', color='000000'),   
                right=Side(border_style='thin', color='000000'),   
                top=Side(border_style='thin', color='000000'),     
                bottom=Side(border_style='thin', color='000000')) 
    
    # rutas_activas = rutas_en_activo_schema(result)

    rutas_activas = body

    # Crear un libro de Excel y seleccionar la hoja activa
    libro_excel = Workbook()
    hoja = libro_excel.active
    hoja.title = 'Hoja1'
  
    # Estilo para el texto en negrita
    negrita = Font(bold=True, size=20,  color='000000')
    # hoja.merge_cells('A1:D1')
    hoja.append(("Ruta : "+nombre_ruta,))
    hoja.append(("Patente : "+patente,))

    # "Patente : "+patente, "driver : "+driver

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

    for celda in hoja[2]:
        celda.font = negrita

    for celda in hoja[4]:
        celda.font = Font(bold=True, color="FFFFFF")
        celda.fill = PatternFill(start_color="000000FF", end_color="000000FF", fill_type="solid")
        celda.border = border
    
    # print(nHoja)
    for n in range(nHoja):
         for celda in hoja[n+5]:
             celda.font = Font(bold=True, color="070707")
             celda.fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
             celda.border = border
    # 
    
    # Ajustar el ancho de las columnas para las celdas de datos (filas después de la 2)
    for columna_cells in hoja.iter_cols(min_row=3):  # Ajustar solo las filas desde la tercera fila en adelante
        longitud_maxima = 0
        for cell in columna_cells:
            if cell.value:
                longitud = len(str(cell.value))
                if longitud > longitud_maxima:
                    longitud_maxima = longitud
        ajuste_ancho = (longitud_maxima + 2) * 1.2
        hoja.column_dimensions[columna_cells[0].column_letter].width = ajuste_ancho

    # Ajustar el ancho de las columnas para las celdas fusionadas (primera y segunda fila)
    for cell_range in hoja.merged_cells.ranges:
        min_col, min_row, max_col, max_row = cell_range.bounds
        if min_row >= 3:  # Ajustar solo las filas desde la tercera fila en adelante
            for columna_index in range(min_col, max_col + 1):
                longitud_maxima = max(len(str(cell.value)) for cell in hoja[hoja.cell(row=min_row, column=columna_index):hoja.cell(row=max_row, column=columna_index)])
                ajuste_ancho = (longitud_maxima + 2) * 1.2
                hoja.column_dimensions[hoja.cell(row=min_row, column=columna_index).column_letter].width = ajuste_ancho

    hoja.append(("",)) 
    hoja.append(("","Driver : "+driver,))  
    hoja.append(("",))  
    hoja.append(("","Firma : ______________________"))    

    hoja.merge_cells('A1:M1')
    hoja.merge_cells('A2:M2')

  # Fusionar celdas para las últimas cuatro filas
    # Guardar el archivo
    nombre_archivo = "nombre_ruta.xlsx"
    libro_excel.save(nombre_archivo)


    return FileResponse("nombre_ruta.xlsx")

@router.post("/asignar")
async def asignar_ruta_activa(asignar : RutasAsignadas):
    try:
        asignar.id_ruta = conn.get_id_ruta_activa_by_nombre(asignar.nombre_ruta)[0]
        print(asignar)
        data = asignar.dict()
        connHela.insert_ruta_asignada(data)

        return {"message": "ruta asignada correctamente"}
    except:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error al ingresar la ruta ")
    

@router.get("/buscar_patente")
async def get_ruta_activa_by_nombre(nombre_ruta: str):

    try:
        results = connHela.read_id_ruta_activa_by_nombre(nombre_ruta)
        print(results)
        if results is None:
            return { "OK": False}

        return driver_ruta_asignada(results)
    except:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error al ingresar la ruta ")



@router.put("/actualizar/ruta_asignada")
async def update_ruta_asignada(body :RutasAsignadas):
    try:
          connHela.update_ruta_asignada(body.patente,body.conductor,body.nombre_ruta)
          return { "message": "Ruta Actualizada Correctamente" }
    except:
          print("error")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")


@router.get("/beetrack/{id_ruta}/descargar")
async def descargar_archivo_beetrack(id_ruta : int):
    results = conn.read_datos_descarga_beetrack(id_ruta)

    wb = Workbook()
    ws = wb.active
    print("/rutas/beetrack/descargar")
    # results.insert(0, ("",))
    results.insert(0, ("NÚMERO GUÍA *","VEHÍCULO *","NOMBRE ITEM *","CANTIDAD","CODIGO ITEM","IDENTIFICADOR CONTACTO *","NOMBRE CONTACTO", "TELÉFONO","EMAIL CONTACTO","DIRECCIÓN *",
    "LATITUD","LONGITUD","FECHA MIN ENTREGA","FECHA MAX ENTREGA","CT DESTINO","DIRECCION","DEPARTAMENTO","COMUNA","CIUDAD","PAIS","EMAIL","Fechaentrega","fechahr",
    "conductor","Cliente","Servicio","Origen","Región de despacho","CMN","Peso","Volumen", "Bultos","ENTREGA","FACTURA","OC","RUTA", "TIENDA"))

    for row in results:
        # print(row)
        ws.append(row)

    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter# get column letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column].width = adjusted_width
    results.insert(0, ("",))
    wb.save("excel/prueba_beetrack.xlsx")

    return FileResponse("excel/prueba_beetrack.xlsx")

@router.get("/recuperar/tracking")
async def recuperar_tracking_beetrack(codigo : str):

    results = conn.recuperar_track_beetrack(codigo)
    # print(results)
    if results == []:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Codigo no encontrado")
    
    return recuperar_tracking_beetrack_schema(results)

@router.get("/recuperar/linea/producto")
async def recuperar_linea_producto(codigo : str):

    results = conn.recupera_linea_producto(codigo)
    # print(results)
    if results == []:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Codigo no encontrado")
    
    return recuperar_linea_producto_schema(results)
   
@router.get("/fecha_ingreso_sistema/{cod_pedido}")
async def recuperar_fecha_ingreso_sistema(cod_pedido : str):
    result = conn.recuperar_fecha_ingreso_cliente(cod_pedido)
    if(result == []) :
         print(f"codigo {cod_pedido} no encontrado")
         return {"Fecha_ingreso_sistema":"Sin Fecha de ingreso al sistema"}

    return recuperar_fecha_ingreso_sistema_schema(result)


@router.get("/tiempo")
async def test():
    time.sleep(90)
    return "hola"

# @router.get("/geolocalizacion")
# async def geolocalizar_direccion():

#     time.sleep(2)
#     return "body"

@router.post("/geolocalizacion")
async def geolocalizar_direccion(body : Latlong):
    geolocalizacion = Nominatim(user_agent="backend/1.0")
    # ubicacion = geolocalizacion.reverse(f"{body.lat},{body.lng}",exactly_one=False)
    ubicacion = geolocalizacion.geocode(body.direccion, exactly_one=True)

    if ubicacion is None :
        return "No se encontro la ubicacion"
    body.lat = ubicacion.latitude
    body.lng = ubicacion.longitude
    body.display_name = ubicacion.address
    body.type = ubicacion.raw['type']
    
    return body

