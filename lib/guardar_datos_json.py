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


def cargar_estado(nombre_archivo):
    try:
        with open('json/'+nombre_archivo+'.json', 'r') as f:
            archivo = json.load(f)
            datos_archivos = archivo['datos']
            ultima_ejecucion_datos = datetime.datetime.fromisoformat(archivo['ultima_ejecucion'])
    except FileNotFoundError:
        datos_archivos = None
        ultima_ejecucion_datos = None
    
    return datos_archivos, ultima_ejecucion_datos