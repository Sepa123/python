from fastapi import APIRouter, status,HTTPException,Header,Depends 
from typing import List , Dict
import re
from decouple import config


##Conexiones
from database.client import reportesConnection 
from datetime import datetime

## Modelos

from database.schema.areati.registro_funciones import registro_funciones_schema
from database.schema.areati.tipo_funcion import tipo_funciones_schema
from database.models.areati.lista_funciones import ListaFuncion

router = APIRouter(tags=["Area_TI"], prefix="/api/areati")

conn = reportesConnection()


@router.post("/agregar/funcion")
async def post_dispatch_guide(body : ListaFuncion):  
    data = body.dict()
    conn.insert_lista_funcion(data)
    return {
        "message": "funcion agregado correctamente"
    }

@router.get("/listar/funcion")
async def get_lista_funcion():
    results = conn.read_lista_funciones()
    return registro_funciones_schema(results)


@router.get("/tipo/funciones")
async def get_lista_funcion():
    results = conn.read_lista_tipo_funciones()
    return tipo_funciones_schema(results)


