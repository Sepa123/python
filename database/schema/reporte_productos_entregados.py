def reporte_producto_schema(reporte_producto):
    return {
        "Dia": reporte_producto[0],
        "Fecha": reporte_producto[1],
        "Electrolux": reporte_producto[2],
        "Sportex": reporte_producto[3],
        "Easy": reporte_producto[4],
        "Easy_OPL": reporte_producto[5]
    }

def reportes_producto_schema(reportes_producto):
    return [reporte_producto_schema(reporte_vehiculo) for reporte_vehiculo in reportes_producto ]