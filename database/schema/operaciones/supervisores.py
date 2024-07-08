def datos_supervisor(operacion):
    return {
        "Id": operacion[0],
        "Nombre": operacion[1],
        "Mail" : operacion[2],
        "Imagen_perfil" : operacion[3],
        "Lista_centros" : operacion[4]
    }

def datos_supervisores_schema(operaciones):
    return [datos_supervisor(operacion) for operacion in operaciones]