def nota_venta_producto(venta):
    return {
		"Id" : venta[0],
		"Created_at" : venta[1],
		"Id_venta" : venta[2],
		"Codigo" : venta[3],
		"Unidades" : venta[4],
		"Id_usuario" : venta[5],
		"Ids_usuario" : venta[6],
        "Producto" : venta[7]
	}

def nota_ventas_productos_schema(ventas):
    return [ nota_venta_producto(venta) for venta in ventas]