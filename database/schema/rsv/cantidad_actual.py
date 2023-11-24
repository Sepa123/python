def cant_actual(cant):

    return {
		"Codigo_producto" : cant[0],
		"Total" : cant[1]
	}

def cant_productos_actual_schema(cants):
    return [cant_actual(cant) for cant in cants]