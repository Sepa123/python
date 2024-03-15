def seguimiento_ruta(ruta):
    return {
		"Ruta_beetrack" : ruta[0],
		"Ppu" : ruta[1],
		"Region" : ruta[2],
		"Cliente" : ruta[3],
		"Carga_total" : ruta[4],
		"Fecha_compromiso" : ruta[5],
		"Entregados" : ruta[6],
		"Entregado_fec_comp" : ruta[7],
		"Pendientes" : ruta[8],
		"No_entregados" : ruta[9],
		"Obs_total_pedidos" : ruta[10]
	}


def seguimiento_ruta_schema(rutas):
    return [ seguimiento_ruta(ruta) for ruta in rutas]