def catalogo_producto(producto):
    return {
		"Id" : producto[0],
		"Created_at" : producto[1],
		"Codigo" : producto[2],
		"Producto" : producto[3],
		"Unid_x_paquete" : producto[4],
		"Peso" : producto[5],
		"Ancho" : producto[6],
		"Alto" : producto[7],
		"Largo" : producto[8],
		"Id_user" : producto[9],
		"Ids_user" : producto[10],
		"Color" : producto[11],
		"Habilitado" : producto[12],
        "Precio_unitario" : producto[13],
		"Ubicacion" : producto[14],
		"Codigo_Original" : producto[15]
	}


def catalogos_productos_schema(productos):
    return [catalogo_producto(producto) for producto in productos]