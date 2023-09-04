def buscar_alerta(alerta):
    return {
		"alerta" : alerta[0],
		"fec_reprogramada" : alerta[1],
		"direccion_correcta" : alerta[2],
		"comuna_correcta" : alerta[3],
		"subestado" : alerta[4],
		"subestado_esperado" : alerta[5],
		"observacion" : alerta[6],
		"codigo1" : alerta[7]
	}

# def buscar_alertas_schema(alertas):
#     return [buscar_alerta(alerta) for alerta in alertas]