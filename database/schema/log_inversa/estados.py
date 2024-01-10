def estado(estado):
    return {
        "Estado": estado[0],
        "Descripcion": estado[1],
    }


def estados_schema(estados):
    return [ estado(est) for est in estados]


def subestado(sub):
    return {
        "Parent_code": sub[0],
        "Nombre": sub[1],
        "Code": sub[2]
    }


def subestados_schema(subestados):
    return [ subestado(sub) for sub in subestados]