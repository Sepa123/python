def ns_driver(driver):
    return {
		"Patente" : driver[0],
		"Total_rutas" : driver[1],
		"Total_km" : driver[2],
		"Total_pedidos" : driver[3],
		"Total_entregados" : driver[4],
		"Total_no_entregados" : driver[5],
		"P_ee" : driver[6],
		"Codigo1" : driver[7]
	}


def ns_drivers_schema(drivers):
    return [ns_driver(driver) for driver in drivers]