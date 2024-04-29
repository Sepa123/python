from fastapi import APIRouter, status,HTTPException, UploadFile, File
from typing import List
import pandas as pd
import os 
import time
from datetime import datetime
##Modelos 

from database.models.retiro_cliente import RetiroCliente

##Conexiones
from database.client import reportesConnection

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

