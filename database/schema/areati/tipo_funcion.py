def tipo_funcion(tipo) : 
    return {
        "Id" : tipo[0],
        "Nombre" : tipo[1]
    }

def tipo_funciones_schema(tipos):
    return [ tipo_funcion(tipo) for tipo in tipos ]