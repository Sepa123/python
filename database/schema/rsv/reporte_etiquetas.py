def reporte_etiqueta(etiqueta):
    return {
		"Carga" : etiqueta[0],
		"Bar_code" :  etiqueta[1],
		"Codigo" :  etiqueta[2],
		"Descripcion" :  etiqueta[3],
		"Color" :  etiqueta[4],
		"Tipo" :  etiqueta[5],
		"En_stock" :  etiqueta[6],
		"Ubicacion" :  etiqueta[7]
	}

def reporte_etiquetas_schema(etiquetas):
    return [ reporte_etiqueta(etiqueta) for etiqueta in etiquetas]
