def centro_operacion(operacion):
    return {
        "Id": operacion[0],
        "Created_at" : operacion[1],
        "Id_user": operacion[2],
        "Ids_user" : operacion[3],
        "Id_op": operacion[4],
        "Centro" : operacion[5],
        "Descripcion" : operacion[6],
        "Region" : operacion[7],
        "Seguimiento_id": operacion[8],
        "Seguimiento_nombre": operacion[9],
    }

def centro_operacion_schema(operaciones):
    return [centro_operacion(operacion) for operacion in operaciones]



def centro_operacion_asignado(operacion):
    return {
        "Id": operacion[0],
        "Created_at" : operacion[1],
        "Id_user": operacion[2],
        "Ids_user" : operacion[3],
        "Id_op": operacion[4],
        "Centro" : operacion[5],
        "Descripcion" : operacion[6],
        "Region" : operacion[7],
        "Estado": operacion[8],
        "Id_ppu_op" : operacion[9]
    }

def centro_operacion_asignado_schema(operaciones):
    return [centro_operacion_asignado(operacion) for operacion in operaciones]




def centro_operacion_usuario(operacion):
    return {
        "Operacion": operacion[0],
        "Centro_operaciones" : operacion[1]
    }


def centro_operacion_usuario_schema(operaciones):
    return [centro_operacion_usuario(operacion) for operacion in operaciones]




def centro_operacion_lista_coordinador(operacion):
    return {
        "Id": operacion[0],
        "Id_op": operacion[1],
        "Centro" : operacion[2],
        "Descripcion" : operacion[3],
        "Region" : operacion[4]
    }

def co_lista_coordinador_schema(operaciones):
    return [centro_operacion_lista_coordinador(operacion) for operacion in operaciones]