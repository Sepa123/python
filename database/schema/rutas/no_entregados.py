def no_entregado(entregado):
    return {
		"Descripcion" : entregado[0],
		"Total" : entregado[1]
	}

def no_entregados_schema(entregados):
    return [no_entregado(entregado) for entregado in entregados]