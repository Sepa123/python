def alerta_conductor_ruta(alerta):
    return {
		"Codigo_pedido" : alerta[0],
		"Codigo_producto" : alerta[1],
		"Posicion" : alerta[2],
		"Alerta_conductor" : alerta[3],
        "Descripcion": alerta[4]
	}

def alertas_conductor_ruta_schema(alertas):
    return [alerta_conductor_ruta(alerta) for alerta in alertas]