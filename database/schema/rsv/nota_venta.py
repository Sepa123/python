def nota_venta(venta):
    return {
		"Id" : venta[0],
		"Created_at" : venta[1],
		"Id_user" : venta[2],
		"Ids_user" : venta[3],
		"Sucursal" : venta[4],
		"Cliente" : venta[5],
		"Direccion" : venta[6],
		"Comuna" : venta[7],
		"Region" : venta[8],
		"Fecha_entrega" : venta[9],
		"Tipo_despacho" : venta[10],
		"Numero_factura" : venta[11],
		"Codigo_ty" : venta[12],
		"Preparado" : venta[13],
        "Fecha_preparado" : venta[14],
        "Entregado" : venta[15],
        "Fecha_despacho" : venta[16],
	}

def notas_ventas_schema(ventas):
    return[nota_venta(venta) for venta in ventas]