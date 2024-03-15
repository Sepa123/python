def ns_por_fecha(ns):
    return {
		"Cliente" : ns[0],
		"Compromiso_real" : ns[1],
		"Entregados" : ns[2],
		"Anulados" : ns[3],
		# "Nivel_servicio" : ns[4]
        "Nivel_servicio" : 0
	}


def ns_por_fecha_schema(nss):
    return [ ns_por_fecha(ns) for ns in nss]