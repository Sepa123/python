from fastapi import APIRouter, status,HTTPException,Header,Depends 
from typing import List , Dict
from decouple import config
import httpx

##Conexiones
from database.client import reportesConnection 
import datetime

## Modelos
from database.schema.confirma_facil.electrolux import datos_confirma_facil_schema

router = APIRouter(tags=["electrolux"], prefix="/api/electrolux")

conn = reportesConnection()

@router.get("/confirma_facil")
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

                    return response.json()
                else:
                    # Si la solicitud no fue exitosa, devolver un error
                    return response.json()
        else:
            # Si la solicitud no fue exitosa, devolver un error
            return {"error": "No se pudo obtener la información del usuario",
                    "body" : response.json()}, response.status_code



    
        # print(body)

## Reportes Historicos
resultado_header_token = None
ultima_ejecucion = None

async def get_token_unico():
    global resultado_header_token
    cf_login = "https://utilities.confirmafacil.com.br/login/login"
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

        timeout = httpx.Timeout(20.0, read=None)
        response = await client.post(url=cf_login,json=body_login)
        # Verificar si la solicitud fue exitosa
        if response.status_code == 200:
            # Si la solicitud fue exitosa, devolver los datos obtenidos
            resp = response.json()
            token_acceso = resp["resposta"]["token"]
            header["Authorization"] = token_acceso

            resultado_header_token = header

        else:

            print("no se puedo recuperar token access")

async def ejecutar_solo_una_vez_por_hora():
    global ultima_ejecucion 

    # Obtener la fecha y hora actual
    ahora = datetime.datetime.now()

    # Verificar si la función ya se ejecutó hoy
    if ultima_ejecucion is None or ahora - ultima_ejecucion > datetime.timedelta(minutes=90):
        # Ejecutar la función
        await get_token_unico()

        # Actualizar el momento de la última ejecución a la hora actual
        ultima_ejecucion = ahora

        print("Ultima Ejecucion Reporte Historico", ultima_ejecucion)


@router.get("/confirma_facil/test")
async def datos_confirma_facil_tesst():
    await ejecutar_solo_una_vez_por_hora()

    return resultado_header_token

@router.get("/confirma_facil/codigo/{codigo}")
async def datos_confirma_facil(codigo : str):
    results = conn.recuperar_data_electrolux()
    datos_cf = datos_confirma_facil_schema(results)

    datos_enviar = []
    for dato in datos_cf:
        if dato["Numero"] == codigo:
            body = {
                    "embarque": {
                        "numero": dato["Factura"],
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

    cf_embarque = "https://utilities.confirmafacil.com.br/business/v2/embarque"
     # Hacer una solicitud a la API externa usando httpx

    timeout = httpx.Timeout(20.0, read=None)
    await ejecutar_solo_una_vez_por_hora()

    async with httpx.AsyncClient() as client:
        response = await client.post(url=cf_embarque,json=datos_enviar,headers=resultado_header_token,timeout=timeout)
        # Verificar si la solicitud fue exitosa
        if response.status_code == 200:
            # print("Electrolux ",response.json())
            print("si paso cf")
            return response.json()
        elif response.status_code == 400:
            # Si la solicitud no fue exitosa, devolver un error
             print("cf : la ocurrencia ya existe")
             return {
                "message" : "No paso, la ocurrencia ya existe"
             }
        else:
            return {
                "message": "Hubo Otro errors"
            }
