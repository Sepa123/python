from fastapi import APIRouter, status,HTTPException,Header,Depends
from typing import List
import re
from decouple import config

##Conexiones
from database.client import reportesConnection , UserConnection
from database.hela_prod import HelaConnection
from datetime import datetime

## Modelos

from database.models.beetrack.dispatch_guide import DistpatchGuide
from database.models.beetrack.dispatch import Dispatch
from database.models.beetrack.route import Route


router = APIRouter(tags=["Beetrack"], prefix="/api/beetrack")

conn = reportesConnection()
connHela = HelaConnection()
connUser = UserConnection()

# Función de dependencia para validar los encabezados
def validar_encabezados(content_type: str = Header(None), x_auth_token: str = Header(None)):
    # Verificar los valores de los encabezados
    if content_type != "application/json":
        raise HTTPException(status_code=400, detail="Content-Type debe ser application/json")
    if x_auth_token != config("SECRET_KEY"):
        raise HTTPException(status_code=401, detail="X-AUTH-TOKEN inválido")
    return content_type, x_auth_token



@router.post("/dispatch_guide")
async def post_dispatch_guide(body: DistpatchGuide, headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers

    return {
            "header" :  {"Content-Type": content_type, "X-AUTH-TOKEN": x_auth_token},
            "body" : body
            }

@router.post("/dispatch")
async def post_dispatch(body : Dispatch, headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers
    return {
            "header" :  {"Content-Type": content_type, "X-AUTH-TOKEN": x_auth_token},
            "body" : body
            }

@router.post("/route")
async def post_route(body : Route , headers: tuple = Depends(validar_encabezados)):
    content_type, x_auth_token = headers
    return {
            "header" :  {"Content-Type": content_type, "X-AUTH-TOKEN": x_auth_token},
            "body" : body
            }