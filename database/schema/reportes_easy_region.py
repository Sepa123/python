def reporte_easy_region_schema(reporte_easy):
    return {
        "Origen": reporte_easy[0],
        "R_metropolitana" : reporte_easy[1],
        "V_region" : reporte_easy[2],
    }

def reportes_easy_region_schema(reportes_easy):
    return [reporte_easy_region_schema(reporte_easy) for reporte_easy in reportes_easy]  