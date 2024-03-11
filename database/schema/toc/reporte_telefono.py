def reporte_telefono(reporte):
    return {
		"Ingreso_sistema" : reporte[0],
		"Cliente" :  reporte[1],
		"Telefono" :  reporte[2],
		"Cod_pedido" :  reporte[3],
		"Fecha_compromiso" :  reporte[4],
		"Cod_producto" :  reporte[5],
		"Sku" :  reporte[6],
		"Nombre" :  reporte[7],
		"Direccion_real" : reporte[8],
		"Comuna" :  reporte[9],
		"Region" :  reporte[10],
		"Cantidad" : reporte[11],
		"Verificado" :  reporte[12],
		"Recepcionado" :  reporte[13],
		"Estado" :  reporte[14],
		"Subestado" :  reporte[15]
	}

def reporte_telefonos_schema(reportes):
    return [ reporte_telefono(reporte) for reporte in reportes]