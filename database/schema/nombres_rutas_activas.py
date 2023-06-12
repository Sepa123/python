def nombre_ruta_activas(nombre_ruta):
    return {
        "Nombre_ruta" : nombre_ruta[0]
    }

def nombres_rutas_activas_schema(nombres_ruta):
    return [nombre_ruta_activas(nombre_ruta) for nombre_ruta in nombres_ruta]