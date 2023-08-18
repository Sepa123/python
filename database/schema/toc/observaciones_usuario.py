def observacion_usuario(obs):
    return {
        "Guia" : obs[0],
		"Cliente" : obs[1],
		"Comuna" : obs[2],
		"Fecha_compromiso" : obs[3],
		"Fecha_Reprogramada" : obs[4],
		"Observacion" : obs[5],
		"Codigo_TY" : obs[6],
		"Alerta" : obs[7],
		"En_ruta" : obs[8]
    }

def observaciones_usuario_schema(obss):
    return [observacion_usuario(obs) for obs in obss]