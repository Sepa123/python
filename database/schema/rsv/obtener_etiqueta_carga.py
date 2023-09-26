def obtener_etiqueta_carga(etiqueta):
    return {
		"Bar_code" : etiqueta[0],
		"Codigo_imp" : etiqueta[1],
		"Descripcion" : etiqueta[2],
		"Color" :etiqueta[3],
	}

def obtener_etiquetas_carga_schema(etiquetas):
    return [obtener_etiqueta_carga(etiqueta) for etiqueta in etiquetas]