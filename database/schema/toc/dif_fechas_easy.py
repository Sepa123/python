def dif_fecha_easy(dif):
    return {
		"Cliente" : dif[0],
		"Ingreso_sistema" : dif[1],
		"Fecha_compromiso" : dif[2],
		"Ultima_actualizacion" : dif[3],
		"Dias_ejecucion" : dif[4],
		"Cod_pedido" : dif[5],
		"Id_entrega" : dif[6],
		"Direccion" : dif[7],
		"Comuna" : dif[8],
		"Descripcion" : dif[9],
		"Unidades" : dif[10],
		"Estado" :dif[11],
		"Subestado" : dif[12]
	}


def dif_fecha_easy_schema(difs):
    return [ dif_fecha_easy(dif) for dif in difs]