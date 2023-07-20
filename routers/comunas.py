from fastapi import APIRouter, status,HTTPException

##Modelos 

from database.schema.comunas.regiones import regiones_schema

##Conexiones

from database.comunas import ComunasConnection

router = APIRouter(tags=["Lista_Comunas"], prefix="/api/lista")

connComuna = ComunasConnection()


@router.get("/region", status_code=status.HTTP_202_ACCEPTED)
async def get_pedidos_tiendas_easy_opl():
    results = connComuna.get_regiones_chile()
    return regiones_schema(results)