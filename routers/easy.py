from fastapi import APIRouter, status,HTTPException,Header,Depends 
from typing import List , Dict ,Union
import re
from decouple import config
import lib.beetrack_data as data_beetrack
import httpx

##Conexiones
from database.client import reportesConnection , UserConnection
from database.hela_prod import HelaConnection
from datetime import datetime

## Modelos

from database.models.beetrack.dispatch_guide import DistpatchGuide
from database.models.beetrack.dispatch import Dispatch , DispatchInsert
from database.models.beetrack.route import Route


router = APIRouter(tags=["Easy"], prefix="/api/easy")

conn = reportesConnection()

# Función de dependencia para validar los encabezados
def validar_encabezados(content_type: str = Header(None), auth_token: str = Header(None)):
    # Verificar los valores de los encabezados
    if content_type != "application/json":
        raise HTTPException(status_code=400, detail="Content-Type debe ser application/json")
    if auth_token != config("SECRET_KEY"):
        print("error con token")
        raise HTTPException(status_code=401, detail="auth-token inválido")
    return content_type, auth_token


@router.post("/daas")
async def post_route(body : Union[Dict, List[Dict]] ,  headers: tuple = Depends(validar_encabezados) ):
    print("/easydaas")
    print("estos son los datos que recibe de easy daas")
    print(body)
    return {
            "message" : "datos recibidos"
            }