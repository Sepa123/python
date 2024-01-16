def lista_producto(ruta):
    return {
		"Fecha_ruta" : ruta[0],
		"Codigo_cliente" : ruta[1],
		"Nombre" : ruta[2],
		"Ciudad" : ruta[3],
		"Codigo_pedido" : ruta[4],
		"Codigo_producto" : ruta[5],
		"SKU" : ruta[6],
		"Fecha_pedido" : ruta[7],
		"Descripcion_producto" :ruta[8],
		"Cantidad_producto" : ruta[9],
		"Notas" : ruta[10],
		"Estado" : ruta[11]
	}


def lista_productos_schema(rutas):
    return [ lista_producto(ruta) for ruta in rutas]