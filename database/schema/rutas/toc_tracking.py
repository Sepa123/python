def toc_tracking(toc):
    return {
		"Fecha_Hora" : toc[0],
		"Comuna" : toc[1],
		"Direccion" : toc[2],
		"Fecha_Compromiso" : toc[3],
		"Observacion" : toc[4],
		"Codigo_TY": toc[5],
		"Creado_por" : toc[6]
	}

def toc_tracking_schema(tocs):
    return [ toc_tracking(toc) for toc in tocs]