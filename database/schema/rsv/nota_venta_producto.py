def nota_venta_producto(venta):
    return {
		"Id" : venta[0],
		"Id_venta" : venta[1],
		"Codigo" : venta[2],
		"Unidades" : venta[3],
        "Producto" : venta[4]
	}

def nota_ventas_productos_schema(ventas):
    return [ nota_venta_producto(venta) for venta in ventas]