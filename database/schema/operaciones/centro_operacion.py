def centro_operacion(operacion):
    return {
        "Id": operacion[0],
        "Created_at" : operacion[1],
        "Id_user": operacion[2],
        "Ids_user" : operacion[3],
        "Id_op": operacion[4],
        "Centro" : operacion[5],
        "Descripcion" : operacion[6],
        "Region" : operacion[7]
    }

def centro_operacion_schema(operaciones):
    return [centro_operacion(operacion) for operacion in operaciones]