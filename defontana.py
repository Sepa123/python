
from datetime import datetime, timedelta
##Conexiones
from database.client import reportesConnection
import json
# import datetime
import httpx

conn = reportesConnection()

access_token = None
ultima_ejecucion_token = None

def guardar_estado(token, hora):
    estado = {'access_token': token, 'ultima_ejecucion_token': str(hora)}
    with open('estado.json', 'w') as f:
        json.dump(estado, f)

def cargar_estado():
    try:
        with open('estado.json', 'r') as f:
            estado = json.load(f)
            access_token = estado['access_token']
            ultima_ejecucion_token = datetime.fromisoformat(estado['ultima_ejecucion_token'])
    except FileNotFoundError:
        access_token = None
        ultima_ejecucion_token = None
    
    return access_token, ultima_ejecucion_token

# Dentro de tu función ejecutar_api_defontana(), después de obtener el token y la hora actual:

def ejecutar_api_defontana():
    global access_token
    global ultima_ejecucion_token

    access_token, ultima_ejecucion_token = cargar_estado()
    # fecha_actual = datetime.datetime.now()

    # Formatear la fecha en el formato deseado (YYYY-MM-DD)
    
    count = 0
    # while True:
    print("inicion")

    # Obtener la fecha y hora actual
    ahora = datetime.now()
    # Verificar si la función ya se ejecutó hoy
    if access_token is None or (ultima_ejecucion_token is None or ahora - ultima_ejecucion_token > timedelta(minutes=120)):
        url_login = 'https://replapi.defontana.com/api/Auth?client=&company=&user=&password='
        # client = httpx.Client()
        print("necesito un token")
        login = httpx.get(url_login, timeout=30)
        body_login = login.json()
        access_token = body_login['access_token']

        
        ultima_ejecucion_token = ahora

        print("Ahora:", ahora)
        print("ultima ejecucion auth:", ultima_ejecucion_token)
    fecha_formateada = ahora.strftime("%Y-%m-%d")

    reqUrl = f"https://replapi.defontana.com/api/sale/GetSaleByDate?initialDate={fecha_formateada}&endingDate={fecha_formateada}&itemsPerPage=50&pageNumber=1"

    if access_token is None:
        print("Por algun motivo no recupero token")
        access_token = None
    else:
        print("ya existe un token")
        authorization = "Bearer "+access_token
        headersList = {
        "Accept": "*/*",
        "Authorization": authorization      
        }

        data = httpx.get(reqUrl, headers=headersList, timeout=30)
        body = json.loads(data.text)
        lista_venta = body["saleList"]
        total_items = body["totalItems"]
        # print(lista_venta)
        print('total',total_items)
        print("termino")
        count = count +1

        if total_items is None or total_items == 0:
            print("no hay registros, chao")

        else:
            lista_folios = []
            for folio in lista_venta:
                lista_folios.append(str(folio['firstFolio']))

            folios_registrados = conn.revisar_datos_folio(', '.join(lista_folios))

            print(folios_registrados)
        
            for venta in lista_venta:
                if str(venta['firstFolio']) in folios_registrados:
                    print("ya esta registrado")
                else:
                    print("sin registrar")
                    conn.insert_venta_defontana(venta)
                    for detalle in venta['details']:
                        conn.insert_detalle_venta_defontana(detalle,venta['firstFolio'])      
                        
        guardar_estado(access_token, ultima_ejecucion_token)             
        



ejecutar_api_defontana()