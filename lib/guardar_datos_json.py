from database.client import reportesConnection 
import datetime
import json
from decimal import Decimal
# import datetime
import os

def ejecutar_por_minutos(minutos, nombre_archivo):
    global ultima_ejecucion 

    datos_archivos = None
    ultima_ejecucion = None

    try:
        with open('json/'+nombre_archivo+'.json', 'r') as f:
            archivo = json.load(f)
            datos_archivos = archivo['datos']
            ultima_ejecucion = datetime.datetime.fromisoformat(archivo['ultima_ejecucion'])
    except FileNotFoundError:
        datos_archivos = None
        ultima_ejecucion = None

    # Obtener la fecha y hora actual
    ahora = datetime.datetime.now()

    # Verificar si la función ya se ejecutó hoy
    if ultima_ejecucion is None or ahora - ultima_ejecucion > datetime.timedelta(minutes=minutos):

        ultima_ejecucion = ahora
        datos_archivos = None
        
        return datos_archivos
    else:
        return datos_archivos


def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def guardar_datos(datos, hora, nombre_archivo):
    estado = {'datos': datos, 'ultima_ejecucion': str(hora)}
    with open('json/'+nombre_archivo+'.json', 'w') as f:
        json.dump(estado, f, indent=4)

def guardar_datos_a_archivo_existente_cf(datos, hora, nombre_archivo):
    # Ruta del archivo JSON
    ruta_archivo = 'json/' + nombre_archivo + '.json'

    # Verificar si el archivo ya existe
    if os.path.exists(ruta_archivo):
        # Cargar datos existentes
        with open(ruta_archivo, 'r') as f:
            estado_existente = json.load(f)
        
        # Actualizar datos existentes con nuevos datos
        estado_existente['datos'].append(datos)
        estado_existente['ultima_ejecucion'] = str(hora)
        
        # Guardar los datos actualizados
        with open(ruta_archivo, 'w') as f:
            json.dump(estado_existente, f, indent=4)
        
        # print(f"Los datos han sido agregados al archivo {nombre_archivo}.json")
    else:
        # Si el archivo no existe, simplemente guardar los datos como antes
        estado = {'datos': [datos], 'ultima_ejecucion': str(hora)}
        with open(ruta_archivo, 'w') as f:
            json.dump(estado, f, indent=4)
        
        # print(f"Los datos han sido guardados en un nuevo archivo {nombre_archivo}.json")


def cargar_estado(nombre_archivo):
    try:
        with open('json/'+nombre_archivo+'.json', 'r') as f:
            archivo = json.load(f)
            datos_archivos = archivo['datos']
            ultima_ejecucion_datos = datetime.datetime.fromisoformat(archivo['ultima_ejecucion'])
    except FileNotFoundError:
        datos_archivos = None
        ultima_ejecucion_datos = None
        print('????')
    
    return datos_archivos, ultima_ejecucion_datos