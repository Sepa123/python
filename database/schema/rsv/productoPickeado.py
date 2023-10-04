def pickeado_rsv(carga):
    return {
        "Codigo" : carga[0],
        "Descripcion" : carga[1],
        "Ubicacion" : carga[2],
        "Verificado" : carga[3]
	}

def pickeado_rsv_schema(cargas):
    return [ pickeado_rsv(carga) for carga in cargas]