def registro_funcion(registro):
    return  {
            "Esquema" : registro[0],
            "Nombre_funcion" : registro[1],
            "Tipo_retorno": registro[2],
            "Argumentos": registro[3],
            # "Tipo_funcion": registro[4],
            "Codigo_fuente": registro[4],
            }


def registro_funciones_schema(registros):
    return [ registro_funcion(registro) for registro in registros]