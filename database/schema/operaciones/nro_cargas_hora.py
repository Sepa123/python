def nro_carga_hora(carga):
    return {
    "Hora" : carga[0],
    "Nro_carga" : carga[1],
    "Entregas": carga[2],
    "Bultos" : carga[3],
    "Verificados" : carga[4],
    "No_verificados" : carga[5]
}

def nro_cargas_hora_schema(cargas):
    return [nro_carga_hora(carga) for carga in cargas]
