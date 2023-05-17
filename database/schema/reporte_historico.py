def reporte_historico_schema(reporte_historico):
    return {
        "Dia": reporte_historico[0],
        "Fecha": reporte_historico[1],
        "Electrolux": reporte_historico[2],
        "Sportex": reporte_historico[3],
        "Easy": reporte_historico[4],
        "Easy_OPL": reporte_historico[5]
    }

def reportes_historico_schema(reportes_historico):
    return [reporte_historico_schema(reporte) for reporte in reportes_historico ]