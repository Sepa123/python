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
		"Color" : producto[9],
        "Precio_unitario" : producto[10],
		"Ubicacion_p" : producto[11],
        "Ubicacion_u" : producto[12],
        "Codigo_Original" : producto[13],
        "Id_user" : producto[14],
		"Ids_user" : producto[15],
		"Habilitado" : producto[16],
	}


def catalogos_productos_schema(productos):
    return [catalogo_producto(producto) for producto in productos]


def codigo_por_color(codigo):
    return {
        "Codigo" : codigo[0],
		"Producto" : codigo[1],
	}

def codigos_por_color_schema(codigos):
    return [ codigo_por_color(codigo) for codigo in codigos]