from fastapi import APIRouter, status,HTTPException

##Modelos 

from database.schema.comunas.regiones import regiones_schema, comunas_lista_schema

##Conexiones

from database.comunas import ComunasConnection

router = APIRouter(tags=["Lista_Comunas"], prefix="/api/lista")

connComuna = ComunasConnection()


@router.get("/region", status_code=status.HTTP_202_ACCEPTED)
async def get_lista_regiones():
    results = connComuna.get_regiones_chile()
    return regiones_schema(results)


@router.get("/comuna", status_code=status.HTTP_202_ACCEPTED)
async def get_lista_comunas():
    results = connComuna.get_comunas_chile()
    return comunas_lista_schema(results)


@router.get("/comuna/{id_region}", status_code=status.HTTP_202_ACCEPTED)
async def get_lista_comunas_by_id_region(id_region : str):
    results = connComuna.get_comunas_chile_by_id_region(id_region)
    return comunas_lista_schema(results)