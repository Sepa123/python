def buscar_alerta(alerta):
    return {
		"Alerta" : alerta[0],
		"Fecha_Reprogramada" : alerta[1],
		"Direccion_correcta" : alerta[2],
		"Comuna_correcta" : alerta[3],
		"Subestado" : alerta[4],
		"Subestado_esperado" : alerta[5],
		"0bservacion" : alerta[6],
		"Codigo1" : alerta[7]
	}

def buscar_alertas_schema(alertas):
    return [buscar_alerta(alerta) for alerta in alertas]