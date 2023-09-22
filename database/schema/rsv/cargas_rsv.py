def carga_rsv(carga):
    return {
		"id" : carga[0],
		"created_at" : carga[1],
		"fecha_ingreso" : carga[2],
		"id_usuario" : carga[3],
		"ids_usuario" : carga[4],
		"nombre_carga" : carga[5],
		"codigo" : carga[6],
		"color" : carga[7],
		"paquetes" : carga[8],
		"unidades" : carga[9],
		"verificado" : carga[10]
	}


def cargas_rsv_schema(cargas):
    return [ carga_rsv(carga) for carga in cargas]