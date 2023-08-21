def bitacora_usuario(usu):
    return {
        "Cantidad_bitacora" : usu[0],
        "Ids_usuario" : usu[1],
        # "Nombre": usu[2]
    }

def bitacoras_usuarios_schema(usuarios):
    return [bitacora_usuario(usu) for usu in usuarios]