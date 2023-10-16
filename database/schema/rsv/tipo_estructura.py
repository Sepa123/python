def tipo_estructura(est):
    return {
        "Id" : est[0],
        "Tipo" : est[1],
    }


def tipos_estructuras_schema(estructuras):
    return [tipo_estructura(est) for est in estructuras]