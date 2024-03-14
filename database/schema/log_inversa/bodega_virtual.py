def bodega_virtual(bodega):
    return {
		"Ingreso_li" : bodega[0],
		"Entrega" : bodega[1],
		"Fecha_pedido" : bodega[2],
		"Fec_original_pedido" :bodega[3],
		"Descripcion" : bodega[4],
		"Cantidad" : bodega[5],
		"Comuna" : bodega[6],
		"Region" : bodega[7],
		"Pistoleado" : bodega[8]
	}


def bodega_virtual_schema(bodegas):
    return [ bodega_virtual(bodega) for bodega in bodegas]