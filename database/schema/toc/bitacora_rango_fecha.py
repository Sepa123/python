def bitacora_rango_fecha(bitacora):
    return {
		"Fecha_creacion" : bitacora[0],
		"Guia" : bitacora[1],
		"Cliente" : bitacora[2],
		"Comuna" : bitacora[3],
        "Direccion":bitacora[4],
		"Fecha_Compromiso" : bitacora[5],
		# "Fecha_Reprogramada" : alerta[5],
		"Observacion" : bitacora[6],
		"Codigo_TY" : bitacora[7],
		"Alerta" : bitacora[8],
		"En_ruta" : bitacora[9],
		"Estado" : bitacora[10],
        "Created_by" : bitacora[11]
	}

def bitacoras_rango_fecha_schema(bitacoras):
    return [bitacora_rango_fecha(bitacora) for bitacora in bitacoras]