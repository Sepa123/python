def alerta_vigente(alerta):
    return {
		"Fecha_creacion" : alerta[0],
		"Guia" : alerta[1],
		"Cliente" : alerta[2],
		"Comuna" : alerta[3],
        "Direccion":alerta[4],
		"Fecha_Compromiso" : alerta[5],
		# "Fecha_Reprogramada" : alerta[5],
		"Observacion" : alerta[6],
		"Codigo_TY" : alerta[7],
		"Alerta" : alerta[8],
		"En_ruta" : alerta[9],
		"Id" : alerta[7]
	}


def alertas_vigentes_schema(alertas):
    return [alerta_vigente(alerta) for alerta in alertas]