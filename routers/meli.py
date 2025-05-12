from datetime import datetime
from typing import Optional
from fastapi import APIRouter, status,HTTPException, UploadFile, File, Request
# from typing import List
import pandas as pd
import os

import psycopg2 
# import time
# from datetime import datetime
##Modelos 
from database.models.meli.datos_supervisor import DataSupervisor
from database.schema.meli.citacion_activa import citacion_activa_schema
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
async def Obtener_datos(id_ppu:str):
    datos = conn.lista_conductores(id_ppu)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id": fila[0],
                                "nombre_completo": fila[1],
                                "tipo_usuario": fila [2],
                                "celular_formateado": fila[3]
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
async def obtener_citacion_operacion_fecha(fecha: str, id : int):

    try:
        # Obtener la fecha actual del sistema
        fecha_actual = datetime.now().date()  # Convertir a objeto de tipo date
        
        # Validar el formato de la fecha proporcionada
        try:
            fecha_proporcionada = datetime.strptime(fecha, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="El formato de la fecha proporcionada es incorrecto. Use el formato YYYY-MM-DD.")
        

        # Validar si la fecha proporcionada es anterior a la fecha actual
        if fecha_proporcionada < fecha_actual:
            raise HTTPException(status_code=400, detail="La fecha proporcionada es anterior a la fecha actual. El API no se ejecutará.")

        # Ejecutar la consulta utilizando nuestra función
        datos = conn.citacion_operacion_fecha(fecha,id)
        # Verificar si hay datos 
        if datos:
            datos_formateados = [dict(zip(["Id_operacion", "operacion", "id_cop", "nombre_cop", "region", "region_name", "citacion", "confirmados", "pendientes", "rechazadas", "ambulancia"], fila)) for fila in datos]
            return datos_formateados
        else:
            raise HTTPException(status_code=404, detail="No se encontraron datos")
    
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta: {str(e)}")
     
    
@router.delete("/borrar")
async def eliminar_modalidad(id: int, fecha: str):
    # Llamar a la función para ejecutar la sentencia SQL de eliminación
    conn.borrar_patente_citacion(id,fecha)
    return {"message": f"Entrada con ID {id} eliminada correctamente"}


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
                                "id": fila[0],
                                "id_ppu": fila [1],
                                "ppu": fila[2],
                                "ruta_meli": fila[3],
                                "tipo_ruta":fila[4],
                                "estado": fila [5],
                                "id_driver":fila[6],
                                "nombre_driver": fila[7],
                                "id_peoneta": fila [8],
                                "nombre_peoneta":fila[9]
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
async def actualizar_estado(estado: int, id : int, fecha:str):
    try:
        conn.update_estado_patente_citacion(estado,id,fecha)
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
async def actualizar_estado(ruta_meli: int, id : int, fecha: str):
    try:
        conn.update_estado_ruta_meli_citacion(ruta_meli,id,fecha)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.post("/ingresarDriversPeoneta")
async def actualizar_estado(id_driver: int, id_ppu: int, fecha: str, id_peoneta: Optional[int] = None):
    try:
        conn.update_ingresar_driver_peoneta(id_driver, id_ppu, fecha, id_peoneta)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: 
        print(f"Error en la consulta: {e}")  # 🛠️ Muestra el error en consola
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/SaveData")
async def guardar_datos_ruta_ambulancia(ruta_amb_interna: str, id_ppu: int, fecha: str, id_ppu_amb: int, ruta_meli_amb:str):
    try:
        conn.update_citacion_ruta_meli_amb(ruta_amb_interna, id_ppu, fecha, id_ppu_amb, ruta_meli_amb)
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

async def actualizar_estado(tipo_ruta: int, id : int, fecha: str):
    try:
        rows = conn.update_tipo_ruta_citacion(tipo_ruta,id,fecha)
        return {"message": f"Datos Ingresados Correctamente : {rows}"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

def corregir_utf8(texto):
    # Decodificar el texto usando la codificación incorrecta (latin-1)
    try:
        texto_decodificado = texto.encode('latin-1').decode('utf-8')
        return texto_decodificado
    
    except UnicodeDecodeError:
        # Si ocurre un error de decodificación, intentar con 'ignore' para omitir caracteres problemáticos
        # print(texto)
        # texto_decodificado = texto.encode('latin-1', errors='ignore').decode('utf-8', errors='ignore')

        # print('Texto Decodificado',texto )
        
        return texto


##### update 2 15-07-2024

@router.get("/countCitaciones")
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
    

@router.get("/countCitacionesConfirmadas")
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
    

@router.post("/Ambulancia")
async def actualizar_estado(id_ppu_amb: int, ruta_meli_amb:str, ruta_amb_interna:str, id_ppu : int, fecha: str):
    try:
        conn.update_citacion_ambulancia(id_ppu_amb,ruta_meli_amb,ruta_amb_interna,id_ppu,fecha)
        return {"message": "Datos Ingresados Correctamente"}
    
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))


@router.get("/AmbulanceCode")
async def Obtener_dato_ambulanceCode():
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_codigo_ambulancia()
    # Verificar si hay datos 
    if datos:
        datos_formateados = [{
                                "genera_codigo_ambulancia": fila[0],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    


@router.get("/getEstados")
async def Obtener_datos( id_ppu: int, fecha: str):
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_estado_citacion_por_fecha_y_patente(id_ppu,fecha)
    # Verificar si hay datos 
    if datos:
        datos_formateados = [{
                                "tipo_ruta": fila [0],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")



@router.get("/infoAMB")
async def retorno_ambulancia( op: int, cop: int,id_ppu: int, fecha: str):
     # Consulta SQL para obtener datos (por ejemplo)

    # Ejecutar la consulta utilizando nuestra función
    datos = conn.retorno_ambulancia(op,cop,id_ppu, fecha)
    # Verificar si hay datos 
    if datos:
        datos_formateados = [{
                                "id_ppu": fila [0],
                                "ppu": fila[1],
                                "ruta_meli": fila[2],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

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

    # # if len(desc.columns) == 9:
    # #     conn.insert_datos_excel_prefactura_meli(id_usuario,ids_usuario,id_prefect,periodo,lista_desc)
    # # else:
    # #     ###en caso de que falte un campo ( total) uso este para insertar los datos
    # #     conn.insert_datos_excel_prefactura_meli_minus(id_usuario,ids_usuario,id_prefect,periodo,lista_desc)

    
    conn.insert_datos_excel_prefactura_meli(id_usuario,ids_usuario,id_prefect,periodo,lista_desc)

    mensaje = conn.ejecutar_funcion_tabla_paso_prefactura()

    return {
        "message" : f'insertados: {mensaje[0]} duplicados : {mensaje[1]}'
    }


@router.post("/subir/prefactura/diario", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo_prefactura_meli_diario(id_usuario : str,ids_usuario : str,latitud : str,longitud : str,file: UploadFile = File(...)):

    try:
        # Obtener la fecha actual
        fecha_actual = datetime.now()

        # Formatear la fecha en el formato 'yyyy-mm-dd'
        fecha_formateada = fecha_actual.strftime('%Y-%m-%d')

        directorio  = os.path.abspath("excel")

        ruta = os.path.join(directorio,file.filename)

        with open(ruta, "wb") as f:
            contents = await file.read()
            # print("pase por aqui")
            f.write(contents)

        df = pd.read_excel(ruta)

        lista = df.to_dict(orient='records')

        fkey = list(lista[0].keys())[0]

        if fkey == 'monitoring-row__bold':
            print('es un LM')
            conn.insert_datos_excel_prefactura_meli_diario_lm(id_usuario,ids_usuario,fecha_formateada,latitud,longitud,lista)

        elif fkey == 'monitoring-row-higher-details__text':
            print('es un FM')

            conn.insert_datos_excel_prefactura_meli_diario_fm(id_usuario,ids_usuario,fecha_formateada,latitud,longitud,lista)

        elif fkey == 'list-routes-steps__route-id-title':
            print('es un LH')

            conn.insert_datos_excel_prefactura_meli_diario_lh(id_usuario,ids_usuario,fecha_formateada,latitud,longitud,lista)

        else:
            print('no es ninguno')
            raise HTTPException(status_code=404, detail="El archivo no tiene el formato correcto")
            

        return {
            "message" : 'El archivo se ha subido correctamente'
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/prefacturas")
async def Obtener_datos_excel_prefactura_meli(ano : str, mes : str):

    datos = conn.obtener_datos_excel_prefactura_meli(ano,mes)
    # Verificar si hay datos 
    if datos:

        return prefactura_meli_schema(datos)
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    

@router.get("/prefacturas/limit")
async def Obtener_datos_excel_prefactura_meli_limit(ano : str, mes : str):

    datos = conn.obtener_datos_excel_prefactura_meli_limit(ano,mes)
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

@router.get("/citacion_activa")
async def get_citacion_activa(op: int,cop : int, fecha : str):
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.recupera_data_por_citacion_activa(op,cop, fecha)
    # Verificar si hay datos 
    if datos[0] is None:
        # datos_formateados = citacion_activa_schema(datos)
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    else:
        datos_formateados = datos[0]
        return datos_formateados
        
    


@router.get("/citacion_supervisor")
async def get_citacion_activa(id_usuario: int, fecha : str):
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.recupera_data_por_citacion_supervisor(id_usuario, fecha)
    # Verificar si hay datos 
    if datos[0] is None:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    else:
        datos_formateados = datos[0]

        for info in datos_formateados:

                avance = info['Detalles'][0]['avance']
                sum_avance = 0
                for detalle in info['Detalles']:
                    sum_avance = detalle['p_avance'] + sum_avance

          # Estructura deseada

                prom_avance = round(sum_avance /len(info['Detalles']),2 )

                chart_data = {
                    "labels": [
                        "Avance",
                        "Faltante"
                    ],
                    "datasets": [{
                        "data": [prom_avance, 100 - prom_avance],
                        "backgroundColor": ['#4CAF50', '#e0e0e0'],
                        "hoverOffset": 4
                    }],
                }

                info['chart_data'] = chart_data

    return datos_formateados
    

@router.post("/BitacoraGeneral")

async def actualizar_estado(id_usuario: int, ids_usuario:str, modificacion: str, latitud : float , longitud: float, origen:str):
    try:
        conn.insert_bitacora_meli(id_usuario, ids_usuario, modificacion, latitud, longitud, origen)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))



@router.post("/citacion_supervisores/guardar")
async def guardar_dato_citacion_supervisores(data_supervisor : DataSupervisor):

    
    try:

        contador_fallas = []
        # conn.insert_datos_de_citacion_activa_FM(data_supervisor)
        for datos in data_supervisor.datos:

            ### en caso de tener una id como None, ignorarla y pasar a la siguiente
            if datos.ruta_meli is None:
                # contador_fallas.append(datos.ppu)
                id_ambulancia = conn.get_max_id_meli_ambulancias()[0]

                conn.insert_datos_de_citacion_activa_FM_ambulancia(data_supervisor,datos,id_ambulancia)

                conn.insert_bitacora_meli(data_supervisor.id_usuario, data_supervisor.ids_usuario, f'Ingreso de ambulancia id: {id_ambulancia}', float(data_supervisor.latitud), float(data_supervisor.longitud), 'Citacion_Supervisor')

                # pass

            else:

                existe_id_ruta = conn.verificar_id_ruta_existe(datos.ruta_meli)[0]

                if existe_id_ruta == 0:
                    conn.insert_datos_de_citacion_activa_FM(data_supervisor,datos)

                    conn.insert_bitacora_meli(data_supervisor.id_usuario, data_supervisor.ids_usuario, f'Ingreso de nueva citación', float(data_supervisor.latitud), float(data_supervisor.longitud), 'Citacion_Supervisor')

                else:
                    conn.update_datos_de_citacion_activa_FM(data_supervisor,datos)

                    conn.insert_bitacora_meli(data_supervisor.id_usuario, data_supervisor.ids_usuario, f'Actualización de citación : {datos.ruta_meli} ', float(data_supervisor.latitud), float(data_supervisor.longitud), 'Citacion_Supervisor')


             

        
        if len(contador_fallas) == 0:        
            return {"message": "Datos guardados con éxito"}
        else:
            return {"message": f"Datos guardados pero con {len(contador_fallas)} intento(s) fallidos"}
    
    except Exception as e: 
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
    
    except ValueError as e: 
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/resumen/rutas/supervisor")
async def get_experiencia_comentario(fecha_ini: str, fecha_fin: str, usuario: int):
    datos = conn.resumen_rutas_fecha_sup(fecha_ini, fecha_fin,usuario)

    return datos[0]
    

@router.get("/lista_ppu/fotos")
async def get_lista_ppu_con_fotos(fecha_ini: str, fecha_fin: str):
    datos = conn.lista_ppu_con_fotos(fecha_ini, fecha_fin)

    print(datos)

    if datos[0] is None or datos[0] == 'null':
        return [{
            'Ppu': '',
            'Id_ruta' : ''
        }]
    else:
        return datos[0]



@router.get("/listar/rutas")
async def get_lista_ruta_meli(fecha_ini: str, fecha_fin: str):
    datos = conn.listar_rutas_meli(fecha_ini, fecha_fin)

    return datos[0]


@router.get("/citaciones/panel")
async def panel_citacion_meli(fecha : str):
    datos = conn.panel_citacion_meli(fecha)

    resultado_dict = {titulo.replace(' ','_') : cant for titulo, cant in datos}

    return resultado_dict




@router.get("/image/fotos/{ppu}/{id_ruta}")
async def get_fotos_patentes(ppu: str, id_ruta: int):
    # Obtener el resultado de la consulta
    resultado = conn.get_fotos_patentes(ppu,id_ruta)

    if not resultado:
        raise HTTPException(status_code=404, detail="No se encontraron imágenes para el PPU especificado") 
    
    return {
        "Latitud" : resultado[1],
        "Longitud" : resultado[2],
        "Imagenes": [
                     "https://hela.transyanez.cl/api/camara/image/foto?image_path="+resultado[3],
                     "https://hela.transyanez.cl/api/camara/image/foto?image_path="+resultado[4],
                     "https://hela.transyanez.cl/api/camara/image/foto?image_path="+resultado[5],
                     "https://hela.transyanez.cl/api/camara/image/foto?image_path="+resultado[6]
                     ],
        "Imagen_1" : "https://hela.transyanez.cl/api/camara/image/foto?image_path="+resultado[3],
        "Imagen_2" : "https://hela.transyanez.cl/api/camara/image/foto?image_path="+resultado[4],
        "Imagen_3" : "https://hela.transyanez.cl/api/camara/image/foto?image_path="+resultado[5],
        "Imagen_4" : "https://hela.transyanez.cl/api/camara/image/foto?image_path="+resultado[6],
    }



@router.get("/lista/posibles/rutas")
async def get_lista_posible_ruta_meli(fecha_ini: str, fecha_fin: str):
    datos = conn.get_recupera_posibles_rutas(fecha_ini, fecha_fin)
    
    if datos[0] is None:
        return []
    
    return datos[0]



@router.put("/actualizar/descuentos")

async def actualizar_descuentos(data_supervisor : DataSupervisor):
    try:
        # conn.insert_bitacora_meli(id_usuario, ids_usuario, modificacion, latitud, longitud, origen)

        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))




#### RODRIGO : Carga masiva de dato

parametros_conexion = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT")
}


def ejecutar_consulta(sql):
    try:
        conexion = psycopg2.connect(**parametros_conexion)
        cursor = conexion.cursor()
        cursor.execute(sql)
        filas = cursor.fetchall()
        cursor.close()
        conexion.close()
        return filas
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
def ejecutar_consultaMasiva(sql):
    try:
        conexion = psycopg2.connect(**parametros_conexion)
        cursor = conexion.cursor()
        cursor.execute(sql)
        filas = cursor.fetchall()
        conexion.commit()  # Confirmar la transacción
        cursor.close()
        conexion.close()
        return filas
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def ejecutar_consulta2(sql):
    try:
        conexion = psycopg2.connect(**parametros_conexion)
        cursor = conexion.cursor()
        cursor.execute(sql)
        filas = cursor.fetchall()
        columnas = [desc[0] for desc in cursor.description]  # Obtener nombres de columnas
        cursor.close()
        conexion.close()
        return filas, columnas  # Devolver filas y nombres de columnas
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))   
    


@router.get("/crearCitacionMasiva")
async def obtener_citacion_masiva(patentes: str, op: int, cop: int, fecha: str):
    """
    Ejecuta la consulta para crear una citación masiva en la tabla mercadolibre.crear_citacion_masiva.
    """
    try:
        # Construir la consulta SQL
        consulta = f""" 
            SELECT 	fecha,patente,id_ruta,observacion_patente,patente_valida,observacion_ruta,id_ruta_valido,valido	
            FROM mercadolibre.crear_citacion_masiva_v2('{patentes}', {op}, {cop}, '{fecha}') ccm
        """
        # Ejecutar la consulta utilizando la función ejecutar_consulta
        datos, columnas = ejecutar_consulta2(consulta)  # Obtener filas y columnas
        
        # Verificar si hay datos
        if datos:
            # Formatear los resultados
            datos_formateados = [dict(zip(columnas, fila)) for fila in datos]
            return {"resultados": datos_formateados}
        else:
            raise HTTPException(status_code=404, detail="No se encontraron datos")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta: {str(e)}")

@router.get("/Op/")
async def Obtener_Op():
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = """select * from operacion.modalidad_operacion mo """
    # Ejecutar la consulta utilizando nuestra función
    datos = ejecutar_consulta(consulta)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "centro": fila[4],
                                                
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    
@router.get("/Cop/")
async def Obtener_Cop():
     # Consulta SQL para obtener datos (por ejemplo)
    consulta = """select * from operacion.centro_operacion co  """
    # Ejecutar la consulta utilizando nuestra función
    datos = ejecutar_consulta(consulta)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "id_op": fila[4],
                                "centro": fila[5],
                                                
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.post("/ProcesarCitacionMasiva")
async def procesar_citacion_masiva(request: Request):
    try:
        body_p = await request.json()
        validar_datos(body_p)  # Validar los datos enviados

        # Construir la consulta SQL
        consulta = f"""
            SELECT * 
            FROM mercadolibre.crear_citacion_masiva_destino(
                '{body_p['fecha']}', 
                '{body_p['patentes']}',
                {body_p['op']}, 
                {body_p['cop']}, 
                {body_p['id_user']}
            )
        """
        print(f"Consulta SQL generada: {consulta}")  # 🛠️ Imprime la consulta para depuración

        # Ejecutar la consulta utilizando la función ejecutar_consulta
        datos = ejecutar_consultaMasiva(consulta)  # Obtener solo las filas

        # Verificar si la función devolvió un resultado
        if datos:
            codigo, glosa = datos[0]  # Capturar los valores devueltos por la función
            print(f"Resultado de la función: código={codigo}, glosa={glosa}")  # 🛠️ Depuración
            return {"codigo": codigo, "glosa": glosa}
        else:
            raise HTTPException(status_code=500, detail="La función no devolvió ningún resultado.")
    except Exception as e:
        print(f"Error en la consulta: {e}")  # 🛠️ Muestra el error en consola
        raise HTTPException(status_code=500, detail=f"Error al ejecutar la consulta: {str(e)}")
    
def validar_datos(body_p):
    if not body_p.get('fecha') or not body_p.get('patentes') or not body_p.get('op') or not body_p.get('cop') or not body_p.get('id_user'):
        raise HTTPException(status_code=400, detail="Faltan datos obligatorios.")
    if not validar_patentes(body_p['patentes']):
        raise HTTPException(status_code=400, detail="El formato de las patentes es incorrecto.")

def validar_patentes(patentes: str) -> bool:
    """
    Valida que todas las patentes tengan el formato correcto: 'PATENTE NUMERO'.
    """
    import re
    for linea in patentes.splitlines():
        if not re.match(r"^[A-Z0-9]{6}\s+\d+$", linea.strip()):
            return False
    return True

@router.get("/fechaActual")
async def obtener_fecha_actual_api():
    """
    Endpoint para obtener la fecha actual del sistema.
    """
    fecha_actual = datetime.now().strftime("%d-%m-%Y")
    return {"fecha_actual": fecha_actual}