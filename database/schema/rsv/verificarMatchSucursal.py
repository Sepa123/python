def matchSucursal_rsv(sucursal):
    return {
		"Id" : sucursal[0],
		"bar_code" : sucursal[1],
        "carga" : sucursal[2],
        "id_sucursal": sucursal[3],
        "sucursal" : sucursal[4],
        "stock": sucursal[5]

	}


def match_sucursales_rsv_schema(sucursales):
    return [matchSucursal_rsv(sucursal) for sucursal in sucursales]