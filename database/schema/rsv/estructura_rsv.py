def estructura_rsv(estructura):
    return {
		"Nombre" : estructura[0],
		"Sucursal" : estructura[1],
		"Tipo" : estructura[2],
		"Cantidad_espacios" : estructura[3],
        "Balanceo" : estructura[4],
        "Frontis" : estructura[5]
	}


def estructuras_rsv_schema(estructuras):
    return [ estructura_rsv(estructura) for estructura in estructuras]