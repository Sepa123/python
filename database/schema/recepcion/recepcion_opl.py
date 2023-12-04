def recepcion_opl(opl):
    return {
		"Carga" : opl[0],
		"Codigo_producto" : opl[1],
		"Nombre" : opl[2],
		"Provincia" : opl[3],
		"Calle" : opl[4],
		"Cantidad_producto" : opl[5],
        "Pistoleado" : opl[6],
        "Bultos" : opl[7]
	}


def recepcion_opl_schema(ople):
    return [ recepcion_opl(opl) for opl in ople]