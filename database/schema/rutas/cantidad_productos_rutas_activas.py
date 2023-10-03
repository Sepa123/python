def cant_producto_ruta(producto):
    return  {
		"Cod_pedido" : producto[0],
		"SKU" : producto[1],
		"Bultos" : producto[2]
	}

def cant_productos_rutas_schema(productos):
    return  [cant_producto_ruta(producto) for producto in productos]
   