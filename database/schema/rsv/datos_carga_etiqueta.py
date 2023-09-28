def dato_carga_etiqueta(dato):
    return {
		"Fecha_ingreso" : dato[0],
		"Nombre_carga" : dato[1],
		"Color" : dato[2],
		"Codigo" : dato[3],
		"Producto" : dato[4],
		"Paquetes" : dato[5],
		"Unidades" : dato[6],
		"Total_unidades" : dato[7],
		"Verificado" : dato[8],
		"Cuenta_p" : dato[9],
		"Verifica_p" : dato[10],
		"Cuenta_u" : dato[11],
		"Verifica_u" : dato[12]
	}

def datos_cargas_etiquetas_schema(datos):
    return [ dato_carga_etiqueta(dato) for dato in datos]