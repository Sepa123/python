from fastapi.responses import FileResponse
from openpyxl import Workbook
from openpyxl.styles import Font , PatternFill, Border ,Side
from openpyxl.worksheet.page import PageMargins 

def generar_excel_generico(results, nombre_filas, nombre_excel ):

    # results = conn.obtener_etiqueta_carga_rsv(nombre_carga,codigo,tipo)
    wb = Workbook()
    ws = wb.active
    results.insert(0, nombre_filas)

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
    
    wb.save(f"excel/{nombre_excel}.xlsx")

    return FileResponse(f"excel/{nombre_excel}.xlsx")

def generar_excel_con_titulo(results, nombre_filas, nombre_excel,titulo ):

    # results = conn.obtener_etiqueta_carga_rsv(nombre_carga,codigo,tipo)
    wb = Workbook()
    ws = wb.active

    margins = PageMargins(top=0.3, bottom=0.6, left=0.4, right=0.5, header=0.3, footer=0.3)
    ws.page_margins = margins

    # Estilo para el texto en negrita
    negrita = Font(bold=True, size=20,  color='000000')
    # hoja.merge_cells('A1:D1')
    ws.append((titulo,))
    ws.append(("",))
    results.insert(0, nombre_filas)

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
    

    for celda in ws[1]:
        celda.font = negrita

    ws.merge_cells('A1:K1')

    wb.save(f"excel/{nombre_excel}.xlsx")

    return FileResponse(f"excel/{nombre_excel}.xlsx")



def cambiar_bool(valor):
     if valor is True:
          return "x"
     else:
          return ""