### son nombres distintos a los de la funcion  quadminds.listar_rutas_mensual 
### por conveniencia de la trama
def reporte_ruta_mes(ruta):
    return {
        "start": ruta[0],
        "title" : ruta[1]
    }

def reportes_rutas_mes_schema(rutas):
    return [reporte_ruta_mes(ruta) for ruta in rutas]


def reporte_ruta_dia(ruta): 
    return {
		"Fecha_ruta" : ruta[0],
		"Nombre_ruta" : ruta[1],
		"Ruta_beetrack" : ruta[2],
		"Posicion" : ruta[3],
		"Cod_cliente" : ruta[4],
		"Nombre" : ruta[5],
		"Calle_numero" : ruta[6],
		"Ciudad" : ruta[7],
		"Provincia_estado" : ruta[8],
		"Telefono" : ruta[9],
		"Email" :ruta[10],
		"Cod_pedido" : ruta[11],
		"Fecha_pedido" : ruta[12],
		"Desc_producto" : ruta[13],
		"Cant_producto" : ruta[14],
		"Created_by" : ruta[15],
		"D_embalaje" : ruta[16],
		"D_producto" : ruta[17],
		"Pickado" : ruta[18]
	}

def reporte_ruta_dia_schema(rutas):
    return [reporte_ruta_dia(ruta) for ruta in rutas]