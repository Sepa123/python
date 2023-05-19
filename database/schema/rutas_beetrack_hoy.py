def ruta_beetrack_hoy_schema(ruta_beetrack):
    return {
        "Numero" : ruta_beetrack[0],
        "Ruta" : ruta_beetrack[1],
        "Patente": ruta_beetrack[2],
         "Driver" : ruta_beetrack[3],
        "Inicio" : ruta_beetrack[4],
        "Region" : ruta_beetrack[5],
        "Total_pedidos" : ruta_beetrack[6],
        "Once" : ruta_beetrack[7],
        "Una" : ruta_beetrack[8],
        "Tres" : ruta_beetrack[9],
        "Cinco" : ruta_beetrack[10],
        "Seis" : ruta_beetrack[11],
        "Ocho" : ruta_beetrack[12],
        "Entregados" : ruta_beetrack[13],
        "No_entregados" : ruta_beetrack[14],
        "Porcentaje" : ruta_beetrack[15]
    }

def rutas_beetrack_hoy_schema(rutas_beetrack): 
    return [ruta_beetrack_hoy_schema(ruta_beetrack) for ruta_beetrack in rutas_beetrack]