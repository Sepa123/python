def inventario_sucursal(inventario):
    return {
		"Codigo" : inventario[0],
		"Color" : inventario[1],
		"Producto" : inventario[2],
		"Paquetes" : inventario[3],
		"Unidades" : inventario[4],
		"Total" : inventario[5],
        "Ubicacion" : inventario[6]
	}



def inventarios_sucursal_schema(inventarios):
    return [inventario_sucursal(inventario) for inventario in inventarios]