## rutas activas version 1

def nombre_ruta_activas(nombre_ruta):
    return {
        "Nombre_ruta" : nombre_ruta[0],
        "Estado"      : nombre_ruta[1],
        "Verificado"  : nombre_ruta[2],
        "Alerta"      : nombre_ruta[3],

    }

def nombres_rutas_activas_schema(nombres_ruta):
    return [nombre_ruta_activas(nombre_ruta) for nombre_ruta in nombres_ruta]


## rutas activas version 2

def nombre_ruta_activas_v2(nombre_ruta):
    return {
        "Nombre_ruta" : nombre_ruta[0],
        "Estado"      : nombre_ruta[1],
        "Comunas"     : nombre_ruta[2]
    }

def nombres_rutas_activas_schema_v2(nombres_ruta):
    return [nombre_ruta_activas_v2(nombre_ruta) for nombre_ruta in nombres_ruta]



def comuna_ruta(comuna):
    return comuna[0]
    

def comunas_ruta_schema(comunas):
    return [comuna_ruta(comuna) for comuna in comunas]




#v3 con cregion incluida
def comuna_region_ruta(comuna):
    return {
        "Comuna" : comuna[0],
        "Region" : comuna[1],
    }
    

def comuna_region_rutas_schema(comunas):
    return [comuna_region_ruta(comuna) for comuna in comunas]