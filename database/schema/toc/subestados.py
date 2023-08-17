def subestado(subest):
    return {
		"Id" : subest[0],
		"Parent_code" : subest[1],
		"Nombre" : subest[2],
		"Codigo" :subest[3],
	}


def subestados_schema(subestados):
    return [subestado(subest) for subest in subestados]