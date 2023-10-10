def peso_posicion_suc(suc):
    return {
        "Codigo" : suc [0],
        "Tipo": suc[1],
        "Cantidad" : suc [2],
        "Peso_total": suc[3],
    }

def peso_posicion_sucursales_schema(sucs):
    return [peso_posicion_suc(suc) for suc in sucs]