def citacion_activa(citacion):
    return {
		"estado" : citacion[0],
		"estado_correcto" : citacion[1],
		"ruta_meli" : citacion[2],
		"id_ppu" : citacion[3],
		"razon_id" : citacion[4],
		"ppu" : citacion[5],
		"patente_igual" : citacion[6],
		"driver" : citacion[7],
		"driver_ok" : citacion[8],
		"p_avance" : citacion[9],
		"avance" : citacion[10],
		"campos_por_operacion" : citacion[11],
		"tipo_vehiculo" : citacion[12],
		"valor_ruta" : citacion[13],
		"ruta_cerrada" : citacion[14]
	}

def citacion_activa_schema(citaciones):
    return [citacion_activa(citacion) for citacion in citaciones ]