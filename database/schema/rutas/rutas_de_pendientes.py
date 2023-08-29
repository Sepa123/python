def ruta_de_pendientes(ruta):
    return {
		"origen" : ruta[0],
		"guia" : ruta[1],
		"fec_ingreso" : ruta[2],
		"fec_pedido" : ruta[3],
		"region" : ruta[4],
		"comuna" : ruta[5],
		"descripcion" : ruta[6],
		"cant" : ruta[7],
		"estado" : ruta[8],
		"subestado" : ruta[9],
		"verified" : ruta[10],
		"recepcion" : ruta[11]
	}

def rutas_de_pendientes_schema(rutas):
    return [ruta_de_pendientes(ruta) for ruta in rutas]