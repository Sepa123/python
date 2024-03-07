def producto_agregado(prod):
    return {
		"Ingreso_sistema" : prod[0],
		"Cliente" : prod[1],
		"Anden" : prod[2],
		"Cod_pedido" : prod[3],
		"Fec_compromiso" : prod[4],
		"Cod_producto" : prod[5],
		"Sku" : prod[6],
		"Comuna" : prod[7],
		"Region" : prod[8],
		"Cantidad" : prod[9],
		"Verificado" : prod[10],
		"Recepcionado" : prod[11],
		"Estado" : prod[12],
		"Subestado" : prod[13]
	}


def productos_agregados_schema(difs):
    return [ producto_agregado(dif) for dif in difs]