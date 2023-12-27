def nombre_ruta_activa(ruta):
    return {
        'Nombre_ruta' : ruta[0]
    }

def nombre_rutas_activas_schema(rutas):
    return [nombre_ruta_activa(ruta) for ruta in rutas]
