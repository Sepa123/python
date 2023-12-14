def ns_verificado(ns):
    return {
		"Cliente" : ns[0],
		"Total_registros" : ns[1],
		"Productos_verificados" : ns[2],
		"Nivel_servicio" : f"{ns[3]}%"
	}


def ns_verificados_schema(nss):
    return [ ns_verificado(ns) for ns in nss]