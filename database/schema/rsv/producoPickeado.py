def pickeado_rsv(carga):
    return {
        "Bar_code": carga[0],
        "Descripcion" : carga[1],
        "Codigo" : carga[2],
        "Ubicacion" : carga[3],
        "Verificado" : carga[4]
	}

def pickeado_rsv_schema(cargas):
    return [ pickeado_rsv(carga) for carga in cargas]