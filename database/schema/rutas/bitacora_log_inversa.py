def bitacora_log_inversa(bitacora):
    return {
		"Dia" : bitacora[0],
        "Cliente" : bitacora[1],
		"Codigo_producto" : bitacora[2],
		"Ids_usuario" : bitacora[3],
		"Origen" : bitacora[4],
        "Observacion": bitacora[5]
	}

def bitacora_log_inversa_schema(bitacoras):
    return [bitacora_log_inversa(bitacora) for bitacora in bitacoras]