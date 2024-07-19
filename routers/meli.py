from datetime import datetime
from fastapi import APIRouter, status,HTTPException, UploadFile, File
# from typing import List
import pandas as pd
import os 
# import time
# from datetime import datetime
##Modelos 
import lib.excel_generico as excel
# from database.models.retiro_cliente import RetiroCliente
from database.models.meli.meli import agregarPatente,pv

##Conexiones
from database.client import reportesConnection
from database.schema.meli.prefacturas import prefactura_meli_schema

router = APIRouter(tags=["Clientes"], prefix="/api/meli")

conn = reportesConnection()

@router.post("/subir-archivo", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo(file: UploadFile = File(...)):

    # select quadminds.convierte_en_ruta_manual(1,'202308021040');

    directorio  = os.path.abspath("excel")

    ruta = os.path.join(directorio,file.filename)

    with open(ruta, "wb") as f:
        contents = await file.read()
        # print("pase por aqui")
        f.write(contents)

    df = pd.read_excel(ruta,sheet_name=1,skiprows=1)

    lista = df.to_dict(orient='records')

    for i, data in enumerate(lista):
        # cantidad_encontrada = conn.get_pedido_planificados_quadmind_by_cod_pedido()
        # if cantidad_encontrada[0] >= 1:
        #     print("Producto ya esta registrado") 
        # else:
        # print(data)
        print('posicion',i+1)
        # print(posicion)

    return {
        'message': len(lista)
    }

@router.get("/estados", status_code=status.HTTP_202_ACCEPTED)
async def estados_entregas():
         # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.datos_excel_meli_base()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{"id": fila[0],
                              "created_at": fila[1],
                              "num_semana": fila[2],
                              "route_idd": fila[3],
                              "mlp_name": fila[4],
                              "mlp_lm": fila[5],
                              "patente": fila[6],
                              "fecha": fila[7],
                              "svc": fila[8],
                              "delivery_type": fila[9],
                              "id_dc": fila[10],
                              "xpt_destino": fila[11],
                              "comuna": fila[12],
                              "tipo_vehiculo": fila[13],
                              "distancia_real": fila[14],
                              "distancia_plan": fila[15],
                              "no_visitado": fila[16],
                              "buyer_se_mudo": fila[17],
                              "paquete_danado": fila[18],
                              "rechazo_de_compra": fila[19],
                              "direccion_incorrecta": fila[20],
                              "comprador_ausente": fila[21],
                              "local_cerrado": fila[22],
                              "fuera_de_zona": fila[23],
                              "zona_inaccesible": fila[24],
                              "extravio": fila[25],
                              "robado": fila[26],
                              "perdido": fila[27],
                              "intento_de_robo": fila[28],
                              "bloqueado_por_palabra_clave": fila[29],
                              "otros": fila[30],
                              "spr": fila[31],
                              "paquetes_entregados": fila[32],
                              "paquetes_transferidos": fila[33],
                              "paquetes_recibidos": fila[34],
                              "paquetes_no_entregados": fila[35],
                              "shipments_started": fila[36],
                              "ciclo": fila[37],
                              "servicio": fila[38],
                              "first_stop": fila[39],
                              "last_stop": fila[40],
                              "initial_date": fila[41],
                              "finish_date": fila[42],
                              "hora_inicio": fila[43],
                              "hora_termino": fila[44],
                              "orh": fila[45],
                              "ozh": fila[46],
                              "steam_out": fila[47],
                              "steam_in": fila[48],
                              "ds": fila[49],
                              "vpa": fila[50],
                              "vpa_target": fila[51],
                              "q_early": fila[52],
                              "q_on_time": fila[53],
                              "q_delay": fila[54],
                              "sporh": fila[55],
                              "spozh": fila[56]
                              } for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")



@router.get("/modalidad_operacion")
async def Obtener_datos():
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.buscar_modalidad_operacion()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "nombre": fila [4]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/conductoresList")
async def Obtener_datos(fecha:str):
    datos = conn.lista_conductores(fecha)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id": fila[0],
 	                            "nombre_completo": fila [1]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/peonetaList")
async def Obtener_datos(fecha:str):
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.lista_peonetas(fecha)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id": fila[0],
 	                            "nombre_completo": fila [1]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    
@router.get("/citacionOperacionFecha")
async def Obtener_datos(fecha: str, id : int):
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.citacion_operacion_fecha(fecha,id)
    # Verificar si hay datos 
    if datos:
        datos_formateados = [{
                                "Id_operacion": fila [0],
                                "operacion": fila[1],
                                "id_cop": fila[2],
                                "nombre_cop": fila [3],
                                "region": fila[4],
                                "region_name": fila[5],
                                "citacion": fila[6],
                                "confirmados": fila[7],
	                            "pendientes": fila[8],
	                            "rechazadas": fila[9],
                                "ambulancia": fila[10]

                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
     
    
@router.delete("/borrar")
async def eliminar_modalidad(id_ppu: str):
    # Llamar a la función para ejecutar la sentencia SQL de eliminación
    conn.borrar_patente_citacion(id_ppu)
    return {"message": f"Entrada con ID {id_ppu} eliminada correctamente"}


@router.get("/estadoList")
async def Obtener_datos():
    # # Verificar si hay datos
    datos = conn.lista_estado_citaciones()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id": fila [0],
                                "estado": fila[1]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
### Esto esta repetido
# @router.get("/citacionOperacionFecha")
# async def Obtener_datos(fecha: str, id : int):
#      # Consulta SQL para obtener datos (por ejemplo)
#     consulta = f"select * from mercadolibre.citacion_operacion_fecha('{fecha}', {id});"
#     # Ejecutar la consulta utilizando nuestra función
#     datos = ejecutar_consulta(consulta)
#     # Verificar si hay datos 
#     if datos:
#         datos_formateados = [{
#                                 "Id_operacion": fila [0],
#                                 "operacion": fila[1],
#                                 "id_cop": fila[2],
#                                 "nombre_cop": fila [3],
#                                 "region": fila[4],
#                                 "region_name": fila[5],
#                                 "citacion": fila[6],
#                                 "confirmados": fila[7]

#                             } 
#                             for fila in datos]
#         return datos_formateados
#     else:
#         raise HTTPException(status_code=404, detail="No se encontraron datos")

#### Esto esta repetiod pero lo dejare por ahora
@router.get("/estadoCitacion")
async def Obtener_datos():

    # Ejecutar la consulta utilizando nuestra función
    datos = conn.lista_estado_citaciones()
    #  Verificar si hay datos 
    if datos:
        datos_formateados = [{
                                "id": fila [0],
                                "estado": fila[1]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    
@router.get("/citacion_cop")
async def Obtener_datos(fecha: str, op : int, cop : int):
    datos = conn.recupera_citacion_cop(fecha,op,cop)
    if datos:
        datos_formateados = [{
                                "id_ppu": fila [0],
                                "ppu": fila[1],
                                "ruta_meli": fila[2],
                                "estado": fila [3],
                                "id_driver":fila[4],
                                "nombre_driver": fila[5],
                                "id_peoneta": fila [6],
                                "nombre_peoneta":fila[7]

                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    
    
    
@router.post("/agregarpatente")
async def agregarPatenteCitacion(body: agregarPatente):
    try:
        
        conn.insert_patente_citacion(body)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/actualizar_estadoPpu")
async def actualizar_estado(estado: int, id_ppu : int, fecha:str):
    try:
        conn.update_estado_patente_citacion(estado,id_ppu,fecha)
        print()
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.get("/nombreCitacion")
async def Obtener_datos(id_estado: int):

    datos = conn.estado_citaciones_por_id(id_estado)
    if datos:
        datos_formateados = [{
                                "estado": fila[0]

                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    

@router.post("/actualizar_rutaMeli")

async def actualizar_estado(ruta_meli: int, id_ppu : int, fecha: str):
    try:
        conn.update_estado_ruta_meli_citacion(ruta_meli,id_ppu,fecha)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/ingresarDriversPeoneta")
async def actualizar_estado(id_driver: int, id_peoneta : int, fecha: str, id_ppu:int):
    try:
        conn.update_ingresar_driver_peoneta(id_driver, id_peoneta,fecha,id_ppu)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))




########## update 

@router.get("/patentesPorCitacion")
async def Obtener_datos( op : int, cop : int, fecha: str):
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.recuperar_patentes_citacion(op,cop,fecha)
    # Verificar si hay datos 
    if datos:
        datos_formateados = [{
                                "id_ppu": fila [0],
                                "ppu": fila[1],
                                "tipo": fila[2],
                                "razon_social": fila [3],
                                "colaborador_id": fila [4],
                                "tripulacion": fila[5]

                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/filtro/Cop")
async def Obtener_datos( op : int):
    datos = conn.filtrar_centro_op_por_id_op(op)
    # Verificar si hay datos 
    if datos:
        datos_formateados = [{
                                "id": fila[0],
                                "centro": fila [1]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    
@router.get("/filtroPatentesPorIdOp_y_IdCop")
async def Obtener_datos(id_operacion: str, id_centro_op : int):

    datos = conn.filtrar_citacion_por_op_y_cop(id_operacion,id_centro_op)
    # Verificar si hay datos 
    if datos:
        datos_formateados = [{  
                                "id": fila[0],
                                "fecha": fila [4],
                                "ruta_meli": fila[5],
                                "id_ppu": fila[6],
                                "id_centro_op": fila[8],
                                "tipo_ruta": fila [9],
                                "id_ppu_amb": fila[10],
                                "ruta_meli_amb": fila[11],
                                "ruta_amb_interna": fila[12],
                                "estado": fila[13]
                                

                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")


@router.get("/tipoRuta")
async def Obtener_datos():
    datos = conn.obtener_tipo_ruta_meli()
    # Verificar si hay datos 
    if datos:
        datos_formateados = [{  
                                "id": fila[0],
                                "tipo": fila [1]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    
@router.post("/actualizar_tipoRuta")

async def actualizar_estado(tipo_ruta: int, id_ppu : int, fecha: str):
    try:
        conn.update_tipo_ruta_citacion(tipo_ruta,id_ppu,fecha)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

def corregir_utf8(texto):
    # Decodificar el texto usando la codificación incorrecta (latin-1)
    texto_decodificado = texto.encode('latin-1').decode('utf-8')
    return texto_decodificado

##### update 2 15-07-2024

@router.get("/api/countCitaciones")
async def Obtener_datos(fecha:str, id_cop:int):
        # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.contar_citaciones_co_por_fecha(fecha,id_cop)
    # Verificar si hay datos 
    if datos:
        datos_formateados = [{  
                                "ingresados": fila[0]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    

@router.get("/api/countCitacionesConfirmadas")
async def Obtener_datos(fecha:str, id_cop:int, estado: int):
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.contar_citaciones_co_confirmadas_por_fecha(fecha,id_cop,estado)
    # Verificar si hay datos 
    if datos:
        datos_formateados = [{  
                                "ingresados": fila[0]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    

@router.post("/api/Ambulancia")
async def actualizar_estado(id_ppu_amb: int, ruta_meli_amb:int, ruta_amb_interna:int, id_ppu : int, fecha: str):
    try:
        conn.update_citacion_ambulancia(id_ppu_amb,ruta_meli_amb,ruta_amb_interna,id_ppu,fecha)
        return {"message": "Datos Ingresados Correctamente"}
    
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

def chunk_list(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


@router.post("/subir/billing-meli", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo_billing_meli(id_usuario : int,ids_usuario : str,file: UploadFile = File(...)):

    # select quadminds.convierte_en_ruta_manual(1,'202308021040');

    directorio  = os.path.abspath("excel/prefactura_mensual")

    ruta = os.path.join(directorio,f"{ids_usuario}_"+file.filename)

    with open(ruta, "wb") as f:
        contents = await file.read()
        # print("pase por aqui")
        f.write(contents)

    df = pd.read_excel(ruta)

    lista = df.to_dict(orient='records')

    chunk_size = 10000
    chunks = chunk_list(lista, chunk_size)

    # for chunk in chunks:
    #     # hora_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    #     # # Imprimir la hora actual
    #     # print(f"La hora actual es: {hora_actual}")
    #     conn.insert_datos_excel_prefactura_mensual_meli(id_usuario,ids_usuario,chunk)


    # conn.insert_datos_excel_prefactura_mensual_meli(id_usuario,ids_usuario,lista)



    return {
        "message" : len(lista)
    }




@router.post("/subir/prefactura", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo_prefactura_meli(id_usuario : str,ids_usuario : str,file: UploadFile = File(...)):

    # select quadminds.convierte_en_ruta_manual(1,'202308021040');

    directorio  = os.path.abspath("excel")

    ruta = os.path.join(directorio,file.filename)

    with open(ruta, "wb") as f:
        contents = await file.read()
        # print("pase por aqui")
        f.write(contents)

    df = pd.read_excel(ruta,nrows=1)

    desc = pd.read_excel(ruta, skiprows=4)
    desc = desc.dropna(subset=['DescripciÃ³n'])
    desc = desc[desc['DescripciÃ³n'] != 'Total prefactura:']

    desc["DescripciÃ³n"] = desc["DescripciÃ³n"].apply(corregir_utf8)
    desc["Conductor"] = desc["Conductor"].apply(corregir_utf8)


    lista = df.to_dict(orient='records')
    lista_desc = desc.to_dict(orient='records')

    id_prefect =lista[0]['ID prefactura']
    periodo = lista[0]['Periodo']

    conn.insert_datos_excel_prefactura_meli(id_usuario,ids_usuario,id_prefect,periodo,lista_desc)

    conn.ejecutar_funcion_tabla_paso_prefactura()

    return {
        "message" : len(lista_desc)
    }

@router.get("/prefacturas")
async def Obtener_datos_excel_prefactura_meli(ano : str, mes : str):

    datos = conn.obtener_datos_excel_prefactura_meli(ano,mes)
    # Verificar si hay datos 
    if datos:

        return prefactura_meli_schema(datos)
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    


@router.get("/descargar/prefacturas")
async def Obtener_datos_excel_prefactura_meli(ano : str, mes : str):

    results = conn.obtener_datos_excel_prefactura_meli_descargar(ano,mes)

    

    nombre_filas = ( "Id Usuario", "Ids Usuario" ,"Id Prefactura", "Periodo", "Descripción", "Id de Ruta", "Fecha Inicio", "Fecha Fin","Patente",
                     "Id Patente", "Conductor", "Cantidad", "Precio Unitario","Descuento" ,"Total")
    nombre_excel = f"detalle {ano}-{mes}"


    return excel.generar_excel_generico(results,nombre_filas,nombre_excel)


@router.get("/resumen/prefacturas")
async def Obtener_resumen_prefactura_meli():

    datos = conn.resumen_subida_archivo_prefactura()
    # Verificar si hay datos 
    if datos:

        return {
                    "Total_registros" : datos[0],
                    "Fecha_ultimo_ingreso" : datos[1],
                    "Ultimos_ingresados" : datos[2],
                    "Nombre_ultima_proforma" : datos[3],
                    "Ultimo_segmento_fecha" : datos[4]
                }
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

