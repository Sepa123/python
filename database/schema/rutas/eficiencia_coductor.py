def eficiencia_conductor(ef):
    return {
		"Driver" : ef[0],
		"Patente" : ef[1],
		"Total" : ef[2],
		"Entregados" : ef[3],
		"No_entregado" : ef[4],
		"E_entrega" : ef[5]
	}


def eficiencia_conductor_schema(efs):
    return [ eficiencia_conductor(ef) for ef in efs]