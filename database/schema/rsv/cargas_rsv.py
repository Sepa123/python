def carga_rsv(carga):
    return {
		"Id" : carga[0],
		"Created_at" : carga[1],
		"Fecha_ingreso" : carga[2],
		"Id_usuario" : carga[3],
		"Ids_usuario" : carga[4],
		"Nombre_carga" : carga[5],
		"Codigo" : carga[6],
		"Color" : carga[7],
		"Paquetes" : carga[8],
		"Unidades" : carga[9],
		"Verificado" : carga[10],
        "Etiquetas": carga[11],
        "Sucursal": carga[12],
        
	}


def cargas_rsv_schema(cargas):
    return [ carga_rsv(carga) for carga in cargas]



def lista_carga(carga):
    return {
        "Nombre_carga" : carga[0],
        "Etiquetas": carga[1],
        "Fecha_ingreso": carga[2],
        "Sucursal" : carga[3]
		# "Sucursal" : "ninguna"
	}


def lista_cargas_schema(cargas):
    return [lista_carga(carga) for carga in cargas]