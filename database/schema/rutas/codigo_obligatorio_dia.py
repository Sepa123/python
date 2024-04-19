def codigo_obligatorios_dia(codigo):
    return  {
		"Cliente" : codigo[0],
		"Codigo_pedido" : codigo[1],
		"Fecha_pedido" : codigo[2],
		"fecha_reprogramada" : codigo[3],
		"Comuna" : codigo[4],
		"Region" : codigo[5],
		"Descripcion" : codigo[6],
		"Sin_moradores" : codigo[7],
		"Verificado" : codigo[8],
		"Recepcionado" : codigo[9],
		"Ruta_hela" : codigo[10]
	}

def codigos_obligatorios_dia_schema(codigos):
    return  [codigo_obligatorios_dia(codigo) for codigo in codigos]