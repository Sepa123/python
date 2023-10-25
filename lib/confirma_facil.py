import httpx


async def enviar_datos_confirma_facil(datos_enviar):

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

    print(datos_enviar)

    async with httpx.AsyncClient() as client:
        response = await client.post(url=cf_login,json=body_login)
        # Verificar si la solicitud fue exitosa
        if response.status_code == 200:
            # Si la solicitud fue exitosa, devolver los datos obtenidos
            resp = response.json()
            token_acceso = resp["resposta"]["token"]
            header["Authorization"] = token_acceso

            print(header)

            async with httpx.AsyncClient() as client:
                response = await client.post(url=cf_embarque,json=datos_enviar,headers=header,timeout=60)
                # Verificar si la solicitud fue exitosa
                print(datos_enviar)
                if response.status_code == 200:

                    return response.json()
                else:
                    # Si la solicitud no fue exitosa, devolver un error
                    return response.json()
        else:
            # Si la solicitud no fue exitosa, devolver un error
            return {"error": "No se pudo obtener la información del usuario",
                    "body" : response.json()}, response.status_code