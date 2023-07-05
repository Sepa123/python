from fastapi import APIRouter, status,HTTPException

##Modelos 

from database.models.panel.usuario import Usuario

##Conexiones

from database.client import reportesConnection
from database.hela_prod import HelaConnection

router = APIRouter(tags=["panel"], prefix="/api/panel")

conn = reportesConnection()

connHela = HelaConnection()

@router.post("/registrar", status_code=status.HTTP_201_CREATED)
async def registrar_usuario(usuario : Usuario):
    
    return usuario