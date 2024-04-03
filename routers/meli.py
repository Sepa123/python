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

    df = pd.read_excel(ruta,sheet_name=1,skiprows=4)

    lista = df.to_dict(orient='records')

    for i, data in enumerate(lista):
        # cantidad_encontrada = conn.get_pedido_planificados_quadmind_by_cod_pedido()
        # if cantidad_encontrada[0] >= 1:
        #     print("Producto ya esta registrado") 
        # else:
        print(data)
        # print(posicion)

    return {
        "sos"
    }

    
