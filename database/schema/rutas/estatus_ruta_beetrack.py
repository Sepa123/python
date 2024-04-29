def estatus_ruta_beetrack(ruta):
    return {
		"identificador_ruta" : ruta[0],
		"ppu" : ruta[1],
		"driver" : ruta[2],
		"asignadas" : ruta[3],
		"primera_entrega" : ruta[4],
		"ultima_entrega" : ruta[5],
		"entregados" : ruta[6],
		"no_entregados" : ruta[7],
		"prom_no_ent" : ruta[8],
		"pendientes" : ruta[9],
		"prom_pendientes" : ruta[10],
		"estado_ruta" : ruta[11],
		"cumplimiento" : ruta[12],
		"promedio_x_entrega" : ruta[13],
		"hora_fin_aprox" : ruta[14]
	}


def estatus_ruta_beetrack_schema(efs):
    return [ estatus_ruta_beetrack(ef) for ef in efs]