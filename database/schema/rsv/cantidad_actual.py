def cant_actual(cant):
    paquetes = cant[2]
    unid_x_paquete = cant[4]
    return {
		"Codigo_producto" : cant[0],
		"Total" : (unid_x_paquete * paquetes) + cant[3]
	}

def cant_productos_actual_schema(cants):
    return [cant_actual(cant) for cant in cants]