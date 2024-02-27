def no_entregado(entregado):
    return {
		"Descripcion" : entregado[0],
        "Tag" : entregado[1],
		"Total" : entregado[2]
	}

def no_entregados_schema(entregados):
    return [no_entregado(entregado) for entregado in entregados]