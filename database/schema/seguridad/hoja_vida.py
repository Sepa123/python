def hoja_vida_producto(producto):
    return {
		"Posicion" : producto[0],
		"Icono" : producto[1],
		"Color" : producto[2],
		"Fecha" : producto[3],
		"Hora" : producto[4],
		"Origen" : producto[5],
		"Lat" : producto[6],
		"Lng" : producto[7],
		"Created_by" : producto[8]
	}

def hoja_vida_producto_schema(productos):
    return [hoja_vida_producto(producto) for producto in productos]