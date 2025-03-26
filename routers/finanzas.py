import os
from fastapi import APIRouter, File, UploadFile, status,HTTPException
from typing import List
import re, json

import psycopg2
from database.models.finanza.descuento import DescuentoManual
import lib.excel_generico as excel

##Conexiones
from database.client import reportesConnection , UserConnection
from database.hela_prod import HelaConnection
from datetime import datetime

##Modelos Schemas

router = APIRouter(tags=["Finanzas"], prefix="/api/finanzas")

conn = reportesConnection()


@router.get("/infoTarifas")
async def Obtener_datos():
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_info_tarifario()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "nombre" : fila[0],
                                "valor_inferior": fila[1],
                                "valor_superior": fila [2],
                                "unidad": fila[3],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    
@router.get("/TipoUnidad")
async def Obtener_datos():
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_tipo_unidad()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "unidad": fila[1],
                                "campo_vehiculo": fila [2],
                                "prioridad": fila[3],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    
@router.post("/NuevaTarifa")
async def actualizar_estado(id_usuario:str, ids_usuario: str, nombre:str, valor_inferior:int, valor_superior:int, unidad:int):
    try:
        conn.agregar_nueva_tarifa(id_usuario, ids_usuario, nombre, valor_inferior, valor_superior, unidad)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

## Tarifario General

@router.get("/tarifarioGeneral/getOperacion")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.obtener_modalidad_operaciones()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "nombre": fila[1],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioGeneral/infoTableToSearch")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.obtener_info_tarifario_general_null()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id": fila [0],
                                "operacion" : fila[1],
                                "centro_operacion": fila[2],
                                "tipo_vehiculo":fila[3],
                                "capacidad":fila[4],
                                "periodicidad":fila[5],
                                "tarifa":fila[6],
                                "fecha_de_caducidad":fila[7]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioGeneral/getCentroOperacion")
async def Obtener_datos(id_op: int):
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_centro_operacion(id_op)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "centro": fila[1],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioGeneral/getTipoVehiculo")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = f"""select * from transporte.tipo_vehiculo tv """
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_tipo_vehiculo()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "tipo": fila[1],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioGeneral/GetCaracteristicasTarifa")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.obtener_caract_finanzas()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "nombre": fila[1],
                                "valor_inferior":fila[2],
                                "valor_superior":fila[3],
                                "unidad": fila[4]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioGeneral/getPeriodicidad")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.obtener_periodicidad()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "periodo": fila[1],
                                "descripcion":fila[2],
                                "icono":fila[3]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioGeneral/getInfoTable")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.obtener_info_tarifario_general()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{  
                                "id": fila [0],
                                "nombre" : fila[1],
                                "centro": fila[2],
                                "tipo":fila[3],
                                "caracteristica_tarifa":fila[4],
                                "periodo":fila[5],
                                "tarifa":fila[6],
                                "fecha_de_caducidad":fila[7]


                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioGeneral/CentroFilter")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.obtener_centro_operacion_filter()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "centro": fila[1],
                                "descripcion": fila[2]

                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.post("/tarifarioGeneral/insertDate")
async def actualizar_estado(id:str, fecha_de_caducidad:str):
    try:
        conn.actualizar_fecha_tarifario_general(id, fecha_de_caducidad)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.post("/tarifarioGeneral/NuevaTarifa")
async def actualizar_estado(id_usuario:str, ids_usuario:str, latitud:str, longitud:str, operacion:int, centro_operacion:int, tipo_vehiculo:int, capacidad:int, periodicidad: int, tarifa: int):
    try:
        conn.agregar_nuevo_tarifario_general(id_usuario, ids_usuario, latitud, longitud, operacion, centro_operacion, tipo_vehiculo, capacidad, periodicidad, tarifa)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

### Tarifario Especifico

@router.get("/tarifarioEspecifico/tablaTarifarioEspecifico")
async def ObtenerInformacionTabla():
     # Consulta SQL para obtener datos (por ejemplo)
    # consulta = "select * from finanzas.listar_tarifario_especifico();"
    datos = conn.listar_tarifario_especifico()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "ppu": fila[1],
                                "razon_social": fila[2],
                                "operacion": fila[3],
                                "cop": fila[4],
                                "periodo": fila[5],
                                "tarifa":fila[6],
                                "fecha_de_caducidad": fila[7]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioEspecifico/CentroFilter")
async def datos_cop():
     # Consulta SQL para obtener datos (por ejemplo)
    # consulta = f"""select id, centro, descripcion from operacion.centro_operacion co"""
    datos = conn.datos_cop_tarifario_especifico()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "centro": fila[1],
                                "descripcion": fila[2]

                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.post("/tarifarioEspecifico/insertDateTe")
async def insertarFechaCaducidad(id:str, fecha_de_caducidad:str):
    try:
        conn.actualizar_fecha_tarifario_especifico(id,fecha_de_caducidad)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.post("/tarifarioEspecifico/NuevaTarifa")
async def IngresarTarifarioEspecifico(id_usuario:str, ids_usuario:str, latitud:str, longitud:str, ppu:int, razon_social:int, operacion:int, centro_operacion:int, periodicidad: int, tarifa: int):
    try:
        conn.insert_tarifario_especifico(id_usuario, ids_usuario, latitud, longitud, ppu, razon_social, operacion, centro_operacion, periodicidad, tarifa)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.get("/tarifarioEspecifico/getOperacion")
async def getOperaciones():
     # Consulta SQL para obtener datos (por ejemplo)
    # consulta = "select id, nombre from operacion.modalidad_operacion mo  "
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.datos_op_tarifario_especifico()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "nombre": fila[1],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioEspecifico/getCentroOperacion")
async def select_info_cop(id_op: int):
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = f"""select id,centro from operacion.centro_operacion co where id_op = {id_op}"""
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.datos_cop_tarifario_especifico_por_id(id_op)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "centro": fila[1],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioEspecifico/getTipoVehiculo")
async def selectTipoVehiculo():
     # Consulta SQL para obtener datos (por ejemplo)
    # consulta = f"""select * from transporte.tipo_vehiculo tv """
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_tipo_vehiculo()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "tipo": fila[1],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioEspecifico/getPeriodicidad")
async def ObtenerPeriodo():
     # Consulta SQL para obtener datos (por ejemplo)
    # consulta = f"""select * from finanzas.periodicidad p"""
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_periodicidad()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "periodo": fila[1],
                                "descripcion":fila[2],
                                "icono": fila[3]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioEspecifico/Colaborador")
async def SelectRazonSocial():
     # Consulta SQL para obtener datos (por ejemplo)
    # consulta = f"""select id, razon_social from transporte.colaborador c"""
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.datos_razon_social_tarifario_especifio()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "razon_social": fila[1]

                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioEspecifico/vehiculosXpatente")
async def SelectpatenteFiltrada( id:int):
     # Consulta SQL para obtener datos (por ejemplo)
    # consulta = f"""select id, ppu  from transporte.vehiculo v  where razon_id = {id}"""
    # Ejecutar la consulta utilizando nuestra función
    # The code snippet provided is assigning the value of `conn` to the variable `datos` in Python.
    datos = conn.datos_razon_social_tarifario_especifico(id)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "ppu": fila[1]

                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/tarifarioEspecifico/infoTableToSearchTe")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    # consulta = "SELECT id, operacion, centro_operacion, ppu, periodicidad, tarifa, fecha_de_caducidad FROM finanzas.tarifario_especifico te WHERE fecha_de_caducidad IS NULL;"
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.datos_tarifario_especifico_fecha_null()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id": fila [0],
                                "operacion" : fila[1],
                                "centro_operacion": fila[2],
                                "ppu":fila[3],
                                "razon_social":fila[4],
                                "periodicidad":fila[5],
                                "tarifa":fila[6],
                                "fecha_de_caducidad":fila[7]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    


@router.get("/selecciones/descuentos")
async def get_datos_seleccionables_descuentos():
    datos = conn.datos_seleccionables_descuentos()
    resultado_dict = {titulo : cant for titulo, cant in datos}

    return resultado_dict


@router.post("/subir/archivo", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo(id : str, file: UploadFile = File(...)):

    directorio  = os.path.abspath(f"archivos/finanzas/archivo_adjunto")

    ruta = os.path.join(directorio,file.filename)

    with open(ruta, "wb") as f:
        contents = await file.read()
        f.write(contents)


    conn.agregar_archivo_adjunto_descuento(f'archivos/finanzas/archivo_adjunto/{file.filename}',id)


@router.post("/guardar/descuento", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo(body : DescuentoManual):
    try:
        # directorio  = os.path.abspath(f"finanzas/archivo_adjunto")
        # print(directorio)

        data = body.dict()
        id_desc = conn.get_max_id_descuentos_manuales()[0]

        conn.insert_descuentos_finanzas(data)

        
        ids_origen = str(id_desc) + '-' +'Finanza'

        conn.insert_datos_descuentos(data, id_desc, ids_origen)

        return {
            'message' : 'Descuento registrado exitosamente',
            'id' : id_desc
        }



    except psycopg2.errors.UniqueViolation as error:
            # Manejar la excepción UniqueViolation específica
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: La ruta {body.Ruta} ya se encuentra registrado")

    except Exception as error:
        print(error)
        # Manejar otras excepciones
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error al agregar el descuento.")




