def comuna_por_ruta(ruta):
    return {
        "Nombre_ruta": ruta[0],
        "Comunas": ruta[1],
        "Total_puntos" : ruta[2]
    }

def comunas_por_ruta_schema(rutas):
    return [comuna_por_ruta(ruta) for ruta in rutas]