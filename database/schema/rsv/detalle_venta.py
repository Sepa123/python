def detalle_venta(venta):
    return   {
        "Bar_code" : venta[0],
		"Codigo" : venta[1],
		"Tipo" : venta[2],
		"Cantidad" : venta[3]
    }

def detalle_ventas_schema(ventas):
    return [ detalle_venta(venta) for venta in ventas] 