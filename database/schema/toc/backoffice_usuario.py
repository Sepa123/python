def backoffice_usuario(backoffice):
    return {
		"Fecha_creacion" : backoffice[0],
		"Guia" : backoffice[1],
		"Cliente" : backoffice[2],
		"Comuna" : backoffice[3],
		"Direccion" : backoffice[4],
		"Fecha_compromiso" : backoffice[5],
		"Observacion" : backoffice[6],
		"Codigo_TY" : backoffice[7],
		"Alerta" : backoffice[8],
		"En_ruta" : backoffice[9],
		"Id" : backoffice[10],
		"Creado_por" : backoffice[11]
	}

def backoffices_usuario_schema(backoffices):
    return [backoffice_usuario(backoffice) for backoffice in backoffices]