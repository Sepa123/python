from fastapi import APIRouter, status,HTTPException

##Modelos 

from database.models.retiro_cliente import RetiroCliente

##Conexiones
from database.client import reportesConnection

router = APIRouter(tags=["Clientes"], prefix="/api/clientes")

conn = reportesConnection()


@router.post("/retiro/registrar", status_code=status.HTTP_202_ACCEPTED)
async def registrar_retiro_clientes(retiro_cliente : RetiroCliente):
    results = retiro_cliente
    return results
