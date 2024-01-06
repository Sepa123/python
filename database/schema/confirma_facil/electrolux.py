def dato_confirma_facil(dato):
    # fecha_formateada = dato[2].strftime('%Y/%m/%d')
    # fecha_formateada = dato[2].strftime('%d/%m/%Y')

    return {
		"Tipo_entrega" : dato[0],
		"Numero" : dato[1],
        "Factura" : dato[2],
		"Dt_ocorrencia" : dato[3],
		"hr_ocorrencia" : str(dato[4]),
		"Comentario" : dato[5]
	}

def datos_confirma_facil_schema(datos):
    return [dato_confirma_facil(dato) for dato in datos]