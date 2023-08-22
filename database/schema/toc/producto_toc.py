def buscar_producto_toc_schema(producto):
    return {
		"Fecha" : producto[0],
		"Patente" : producto[1],
		"Guia" : producto[2],
		"Cliente" :producto[3],
		"Region" : producto[4],
		"Estado" : producto[5],
		"Subestado" :producto[6],
		"Usuario_movil" : producto[7],
		"Nombre_cliente" : producto[8],
		"Fecha_compromiso" : producto[9],
		"Comuna" : producto[10],
        "Telefono" : producto[11],
        "Correo": producto[12]
	}