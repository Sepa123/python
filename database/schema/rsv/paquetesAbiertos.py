def paquetes_abiertos_sucursal(inventario):
    return {
        "fecha": inventario[0],
        "id": inventario[1],
        "carga" : inventario[2],
        "bar_code" : inventario[3],
        "codigo" : inventario[4],
        "color": inventario[5],
        "descripcion": inventario[6],
        "tipo": inventario[7]
		

	}

def paquetes_abiertos_sucursal_schema(inventarios):
    return [paquetes_abiertos_sucursal(inventario) for inventario in inventarios]