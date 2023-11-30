def obtener_ubicacion_cantidad(cod):
    return {
		"Codigo" : cod[0],
		"Ubicacion" : cod[1],
		"Paquetes" : cod[2],
		"Unidades" : cod[3]
	}


def obtener_ubicacion_cantidad_schema(cods):
    return [obtener_ubicacion_cantidad(cod) for cod in cods]