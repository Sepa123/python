def tipo_despacho(tipo) :
    return {
        "Id" : tipo[0],
        "Despacho" : tipo[1]
    }


def tipos_despacho_schema(tipos) :
    return [tipo_despacho(tipo) for tipo in tipos]