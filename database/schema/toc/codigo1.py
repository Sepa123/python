def codigo1(codigo):
    return {
        "Id": codigo[0],
        "Descripcion": codigo[1],
        "Descripcion_larga": codigo[2],
    }

def codigos1_schema(codigos):
    return [codigo1(codigo) for codigo in codigos]