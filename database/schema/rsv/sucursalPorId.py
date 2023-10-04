def sucursalPorId_rsv(sucursal):
    return {
		"Id" : sucursal[0],
		"Nombre" : sucursal[1]
	}

def sucursales_rsvPorId_schema(sucursales):
    return [sucursalPorId_rsv(sucursal) for sucursal in sucursales]