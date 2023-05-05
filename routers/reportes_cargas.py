from fastapi import APIRouter,status,HTTPException
from database.client import reportesConnection
from datetime import datetime
from os import remove
from database.models.reportes import cargaEasy_schema

router = APIRouter(prefix="/api/reportes")

conn = reportesConnection()

@router.get("/cargas_easy")
async def get_cuenta():
    data_db = conn.read_cargas_easy()
    if not data_db:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")
    return cargaEasy_schema(data_db)