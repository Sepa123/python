def pendiente_bodega(pendiente):
    return {
		"Cliente" : pendiente[0],
		"Total" : pendiente[1],
		"Pickeados" : pendiente[2],
		"Sin_pickear" : pendiente[3]
	}

def pendientes_bodega_schema(pedidos):
    return [pendiente_bodega(pedido) for pedido in pedidos]