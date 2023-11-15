from fastapi import APIRouter, status,HTTPException

##Modelos 

from database.models.retiro_cliente import RetiroCliente

##Conexiones
from database.client import reportesConnection

router = APIRouter(tags=["Clientes"], prefix="/api/clientes")

conn = reportesConnection()


@router.post("/retiro/registrar", status_code=status.HTTP_202_ACCEPTED)
async def registrar_retiro_clientes(retiro_cliente : RetiroCliente):
    try:
        results = conn.get_ruta_manual(retiro_cliente.Codigo_pedido)
        data = retiro_cliente.dict()
        print("retiro cliente datos :",retiro_cliente)
        # existe_registro = conn.find_retiro_cliente_existente(retiro_cliente.Codigo_pedido)
        # print(existe_registro == [])
        conn.registrar_retiro_cliente(data)
        return results
    except:
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Error")
