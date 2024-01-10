from fastapi import APIRouter, status,HTTPException

##Modelos 

from database.models.retiro_cliente import RetiroCliente
from database.schema.log_inversa.estados import estados_schema,subestados_schema
from database.models.log_inversa.bitacora_lg import BitacoraLg

##Conexiones
from database.client import reportesConnection

router = APIRouter(tags=["LogisticaInversa"], prefix="/api/log_inversa")

conn = reportesConnection()

@router.post("/registrar", status_code=status.HTTP_202_ACCEPTED)
async def registrar_retiro_clientes(bitacora : BitacoraLg):
    # try:
        
        if bitacora.Estado_final == 0:
            bitacora.Subestado_final = None

        print(bitacora)

        data = bitacora.dict()

        conn.inser_bitacora_log_inversa(data)
        rows = conn.update_estados_pendientes(bitacora.Estado_final,bitacora.Subestado_final,bitacora.Codigo_pedido)

        # print(rows)
         
        return {
            "message" : "Orden de compra actualizada correctamente"
        }

    # except:
    #      raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Error")
    

@router.get("/estados", status_code=status.HTTP_202_ACCEPTED)
async def estados_entregas():

    estado = conn.obtener_estados_entrega()
    list_estado = estados_schema(estado)

    subestado = conn.obtener_subestados_entrega()
    list_subestado = subestados_schema(subestado)

    json = {
        "estado" : list_estado,
        "subestado" : list_subestado,
    }

    return json