def actividad_diaria (actividad):
    return {
		"Fecha_creacion" : actividad[0],
		"Guia" : actividad[1],
		"Cliente" : actividad[2],
		"Comuna" : actividad[3],
		"Direccion" : actividad[4],
		"Fecha_compromiso" : actividad[5],
		"Observacion" : actividad[6],
		"Codigo_TY" : actividad[7],
		"Alerta" : actividad[8],
		"En_ruta" : actividad[9],
		"Estado" : actividad[10],
		"Creado_por" : actividad[11]
	}


def actividades_diaria_schema(actividades):
    return [actividad_diaria(actividad) for actividad in actividades]