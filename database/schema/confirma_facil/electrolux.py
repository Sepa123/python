def dato_confirma_facil(dato):
    # fecha_formateada = dato[2].strftime('%Y/%m/%d')
    # fecha_formateada = dato[2].strftime('%d/%m/%Y')

    return {
		"Tipo_entrega" : dato[0],
		"Numero" : dato[1],
		"Dt_ocorrencia" : dato[2],
		"hr_ocorrencia" : str(dato[3]),
		"Comentario" : dato[4]
	}

def datos_confirma_facil_schema(datos):
    return [dato_confirma_facil(dato) for dato in datos]