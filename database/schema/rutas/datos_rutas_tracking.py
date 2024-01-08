def datos_ruta_tracking(ruta):
    return {
        "Codigo_pedido": ruta[0],
        "Fecha_pedido": ruta[1],
        "Fecha_original_pedido" : ruta[2],
        "Codigo_producto": ruta[3],
        "Descripcion_producto": ruta[4],
        "Cantidad_producto": ruta[5],
        "Notas": ruta[6],
        "SKU": ruta[7],
        "Pistoleado": ruta[8],
		"Codigo_cliente": ruta[9],
        "Nombre":  ruta[10],
        "Calle": ruta[11],
        "Direccion_textual": ruta[12],
        "Ciudad": ruta[13],
        "Telefono": ruta[14],
        "Email": ruta[15],
        "Nombre_ruta" : ruta[16]
	}


def datos_rutas_tracking_schema(productos):
    return [datos_ruta_tracking(producto) for producto in productos]