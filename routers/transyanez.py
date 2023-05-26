from fastapi import APIRouter,status
from database.client import transyanezConnection
from fastapi.responses import FileResponse
from openpyxl import Workbook
from datetime import datetime
from os import remove

router = APIRouter(prefix="/api/transyanez")

conn = transyanezConnection()

dia_actual = datetime.today().strftime('%Y-%m-%d')

nombre_archivo = f"resumen_vehiculos_portal_{dia_actual}".format(dia_actual)

# Crear un nuevo libro de trabajo y hoja de cálculo

@router.get("/resumen_vehiculos_portal")
async def get_data():

    results = conn.get_vehiculos_portal()
    wb = Workbook()
    ws = wb.active
    results.insert(0, ("Compañia","Región Origen","Patente","Estado","Tipo","Caracteristicas","Marca","Modelo","Año","Región","Comuna"))

    for row in results:
        ws.append(row)
    
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter # get column letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column].width = adjusted_width
    
    wb.save("resumen_vehiculos_portal_yyyymmdd.xlsx")

    return FileResponse("resumen_vehiculos_portal_yyyymmdd.xlsx")