def estado_transporte (estado):
    return {
        "Id": estado[0],
        "Estado": estado[1]
    }

def estados_transporte_schema(estados):
    return [estado_transporte(estado) for estado in estados]