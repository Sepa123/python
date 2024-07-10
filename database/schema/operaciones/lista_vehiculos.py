def vehiculos_disponibles_op(vehiculo):
    return {
		"Vehiculo_id" : vehiculo[0],
		"Ppu" : vehiculo[1],
		"Tipo" : vehiculo[2],
		"Colaborador_id" : vehiculo[3],
		"Razon_social" : vehiculo[4],
		"Tripulacion" : vehiculo[5]
	}

def vehiculos_disponibles_op_schema(vehiculos):
    return [vehiculos_disponibles_op(vehiculo) for vehiculo in vehiculos]