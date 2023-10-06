def generar_codigo_factura_nota_venta(venta):
    return {
		"Resultado" : venta[0],
		"Mensaje" : venta[1],
		"Codigo" : venta[2]
	}