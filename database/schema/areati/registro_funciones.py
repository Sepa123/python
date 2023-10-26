def registro_funcion(registro):
    return  {
            "Id" : registro[0],
            "Fecha_creacion" : registro[1],
            "Esquema": registro[2],
            "Nombre_funcion": registro[3],
            "Tipo_funcion": registro[4],
            "Descripcion": registro[5],
            "Parametros": registro[6],
            "Comentarios_parametros": registro[7],
            "Palabras_clave": registro[8],
            "Tablas_impactadas": registro[9]
            }


def registro_funciones_schema(registros):
    return [ registro_funcion(registro) for registro in registros]