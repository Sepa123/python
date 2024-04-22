def reporte_hora_schema(reporte_hora):
    return  {
    "Hora": reporte_hora[0],
    "Electrolux": reporte_hora[1], 
    "Easy_CD": reporte_hora[3],
    "Easy_OPL": reporte_hora[4]
    # cambiar Tiendas por Easy_OPL
}

def reportes_hora_schema(reportes_hora):
    return [reporte_hora_schema(reporte_hora) for reporte_hora in reportes_hora]


def reportes_ultima_hora_schema(reportes_hora):
    reporte = [reporte_hora_schema(reporte_hora) for reporte_hora in reportes_hora]
    reporte.pop()
    
    return reporte


    