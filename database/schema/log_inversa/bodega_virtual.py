def bodega_virtual(bodega):
    return {
		"ingreso_li" : bodega[0],
		"entrega" : bodega[1],
		"fecha_pedido" : bodega[2],
		"fec_original_pedido" :bodega[3],
		"descripcion" : bodega[4],
		"cantidad" : bodega[5],
		"comuna" : bodega[6],
		"region" : bodega[7],
		"pistoleado" : bodega[0]
	}


def bodega_virtual_schema(bodegas):
    return [ bodega_virtual(bodega) for bodega in bodegas]