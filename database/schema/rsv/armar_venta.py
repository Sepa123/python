def armar_venta(venta):
    return {
		"Codigo" : venta[0],
		"Cantidad" : venta[1],
		"Paquetes" : venta[2],
		"Unidades" : venta[3],
		"Retorno" : venta[4]
	}

def armar_venta_schema(ventas):
    return [armar_venta(venta) for venta in ventas]