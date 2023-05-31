def carga_easy_comparacion(carga):
    return {
        "Nro_carga" : carga[0],
        "Cantidad" : carga[1]
    }

def cargas_easy_comparacion_schema(cargas):
    return [carga_easy_comparacion(carga) for carga in cargas]