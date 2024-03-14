#Conexiones
from database.client import reportesConnection
import json
# import datetime
import os
import httpx
from decouple import config
## Modelos
from database.schema.confirma_facil.electrolux import datos_confirma_facil_schema
from datetime import timedelta
import datetime

import asyncio


conn = reportesConnection()


async def datos_confirma_facil():
    results = conn.recuperar_data_electrolux()
    datos_cf = datos_confirma_facil_schema(results)

    datos_enviar = []
    for dato in datos_cf:
        
        body = {
                "embarque": {
                    "numero": dato["Numero"],
                    "serie": "02"
                },
                "embarcador": {
                    "cnpj": "761634950"
                },
                "ocorrencia": {
                    "tipoEntrega": dato["Tipo_entrega"],
                    "dtOcorrencia": dato["Dt_ocorrencia"],
                    "hrOcorrencia": dato["hr_ocorrencia"],
                    "comentario": dato["Comentario"],
                    "fotos": []
                }
            }
        datos_enviar.append(body)

    cf_login = "https://utilities.confirmafacil.com.br/login/login"
    cf_embarque = "https://utilities.confirmafacil.com.br/business/v2/embarque"
     # Hacer una solicitud a la API externa usando httpx

    body_login = {
  	 	"email": "admin.area.ti@transyanez.cl",
   	 	"senha": "TYti2022@"
    }


    header = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": ""
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(url=cf_login,json=body_login)
        # Verificar si la solicitud fue exitosa
        if response.status_code == 200:
            # Si la solicitud fue exitosa, devolver los datos obtenidos
            resp = response.json()
            token_acceso = resp["resposta"]["token"]
            header["Authorization"] = token_acceso

            async with httpx.AsyncClient() as client:
                response = await client.post(url=cf_embarque,json=datos_enviar,headers=header,timeout=60)
                # Verificar si la solicitud fue exitosa
                if response.status_code == 200:

                    print(response.json())
                else:
                    # Si la solicitud no fue exitosa, devolver un error
                    # return response.json()
                    print(response.json())
        else:
            # Si la solicitud no fue exitosa, devolver un error
            print({"error": "No se pudo obtener la información del usuario",
                    "body" : response.json()}, response.status_code)
        



# Ejecuta el bucle de eventos
asyncio.run(datos_confirma_facil())