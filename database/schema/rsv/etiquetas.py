def etiqueta_producto(etiqueta):
    return {
		"Id" : etiqueta[0],
		"Created_at" : etiqueta[1],
		"Carga" : etiqueta[2],
		"Bar_code" : etiqueta[3],
		"Codigo" : etiqueta[4],
		"Codigo_imp" : etiqueta[5],
		"Descripcion" : etiqueta[6],
		"Color" :etiqueta[7],
		"Posicion" : etiqueta[8],
		"Tipo" : etiqueta[9],
		"Verificado" : etiqueta[10],
		"En_stock" : etiqueta[11]
	}

def etiquetas_productos_schema(etiquetas):
    return [etiqueta_producto(etiqueta) for etiqueta in etiquetas]



def dato_producto_etiqueta(etiqueta):
    return {
        "Codigo" : etiqueta[0],
        "Descripcion": etiqueta[1],
        "Color": etiqueta[2]
	}


def datos_productos_etiquetas_schema(etiquetas):
    return [dato_producto_etiqueta(etiqueta) for etiqueta in etiquetas]