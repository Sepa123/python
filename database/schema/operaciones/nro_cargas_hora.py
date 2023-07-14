def nro_carga_hora(carga):
    return {
    "Fecha": carga[0],
    "Hora" : carga[1],
    "Cliente": carga[2],
    "Nro_carga" : carga[3],
    "Entregas": carga[4],
    "Bultos" : carga[5],
    "Verificados" : carga[6],
    "No_verificados" : carga[7]
}

def nro_cargas_hora_schema(cargas):
    return [nro_carga_hora(carga) for carga in cargas]
