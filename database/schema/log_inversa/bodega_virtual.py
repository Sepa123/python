def bodega_virtual(bodega):
    return {
		"Ingreso_li" : bodega[0],
        "Cliente": bodega[1],
		"Entrega" : bodega[2],
		"Fecha_pedido" : bodega[3],
		"Fec_original_pedido" :bodega[4],
		"Descripcion" : bodega[5],
		"Cantidad" : bodega[6],
		"Comuna" : bodega[7],
		"Region" : bodega[8],
		"Pistoleado" : bodega[9]
	}


def bodega_virtual_schema(bodegas):
    return [ bodega_virtual(bodega) for bodega in bodegas]