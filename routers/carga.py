from fastapi import APIRouter, status,HTTPException

##Modelos 

from database.models.retiro_cliente import RetiroCliente
from database.schema.cargas.quadminds import cargas_quadminds_schema

##Conexiones
from database.client import reportesConnection
from fastapi.responses import FileResponse
from openpyxl import Workbook
from openpyxl.styles import Font , PatternFill, Border ,Side

router = APIRouter(tags=["Cargas"], prefix="/api/cargas")

conn = reportesConnection()

@router.get("/quadminds" , status_code=status.HTTP_202_ACCEPTED)
async def get_carga_easy_cd():
    results = conn.get_cargas_quadmind()

    return cargas_quadminds_schema(results)

# @router.get("/easy_cd" , status_code=status.HTTP_202_ACCEPTED)
# async def get_carga_easy_cd():
#     # results = conn.get_cargas_easy_cd()

#     # return carga_easy_cd_schema(results)
#     results = conn.get_cargas_easy_cd()
#     wb = Workbook()
#     ws = wb.active
#     results.insert(0,("id","created_at","anden","cd","nro_carga","entrega","fecha_entrega","tipo_orden",
#     "carton","comuna", "region","producto","descripcion","unid","bultos","nombre","direccion","telefono",
#     "correo","compl","cant","verified","estado","subestado","recepcion"
# ))

#     for row in results:
#         ws.append(row)
    
#     for col in ws.columns:
#         max_length = 0
#         column = col[0].column_letter # get column letter
#         for cell in col:
#             try:
#                 if len(str(cell.value)) > max_length:
#                     max_length = len(str(cell.value))
#             except:
#                 pass
#         adjusted_width = (max_length + 2)
#         ws.column_dimensions[column].width = adjusted_width
    
#     wb.save("excel/carga_easy.xlsx")

#     return FileResponse("excel/carga_easy.xlsx")


