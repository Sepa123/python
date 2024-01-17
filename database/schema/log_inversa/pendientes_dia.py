def pendiente(pen):
    return {
		"Nombre_ruta" : pen[0],
		"Region" : pen[1],
		"Patente" : pen[2],
		"Driver" : pen[3],
		"Pendientes" : pen[4]
	}


def pendientes_schema(pendientes):
    return [pendiente(pen) for pen in pendientes]