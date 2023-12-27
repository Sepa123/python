def buscar_producto_ruta(producto):
    return {
		"Codigo_pedido" : producto[0],
		"Codigo_producto" : producto[1],
		"Descripcion_producto" : producto[2],
		"Ciudad" : producto[3],
		"Provincia" : producto[4],
		"Fecha_pedido" : producto[5],
		"Fecha_original_pedido" : producto[6],
		"Nombre_ruta" : producto[7],
        "Notas" : producto[8]
	}


def buscar_productos_ruta_schema(productos):
    return [buscar_producto_ruta(producto) for producto in productos]