def sucursal_rsv(sucursal):
    return {
		"Id" : sucursal[0],
		"Nombre" : sucursal[1],
		"Pais" : sucursal[2],
		"Ciudad" : sucursal[3],
		"Comuna" : sucursal[4],
		"Direccion" : sucursal[5],
		"Latitud" : sucursal[6],
		"Longitud" : sucursal[7],
		"Id_user" : sucursal[8],
		"Ids_user" : sucursal[9]
	}

def sucursales_rsv_schema(sucursales):
    return [sucursal_rsv(sucursal) for sucursal in sucursales]