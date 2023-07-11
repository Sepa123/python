def nro_carga_hora(carga):
    return {
    "Hora" : carga[0],
    "Cliente": carga[1],
    "Nro_carga" : carga[2],
    "Entregas": carga[3],
    "Bultos" : carga[4],
    "Verificados" : carga[5],
    "No_verificados" : carga[6]
}

def nro_cargas_hora_schema(cargas):
    return [nro_carga_hora(carga) for carga in cargas]
