def ruta_de_pendientes(ruta):
    # return {
	# 	"Origen" : ruta[0],
	# 	"Guia" : ruta[1],
	# 	"Fecha_ingreso" : ruta[2],
	# 	"Fecha_pedido" : ruta[3],
	# 	"Region" : ruta[4],
	# 	"Comuna" : ruta[5],
	# 	"Descripcion" : ruta[6],
	# 	"Cantidad" : ruta[7],
	# 	"Estado" : ruta[8],
	# 	"Subestado" : ruta[9],
	# 	"Verificado" : ruta[10],
	# 	"recepcion" : ruta[11]
	# }
	return {
	"Origen": ruta[0],
	"Cod_entrega": ruta[1],
	"Fecha_ingreso": ruta[2],
	"Fecha_compromiso": ruta[3],
	"Region": ruta[4],
	"Comuna": ruta[5],
	"Descripcion": ruta[6],
	"Bultos": ruta[7],
	"Estado": ruta[8],
	"Subestado": ruta[9],
	"Verificado": ruta[10],
	"Recibido": ruta[11]
	}

def rutas_de_pendientes_schema(rutas):
    return [ruta_de_pendientes(ruta) for ruta in rutas]