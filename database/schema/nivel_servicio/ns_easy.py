def ns_easy(driver):
    return {
		"Cliente" : driver[0],
		"Guia" : driver[1],
		"Ruta_hela" : driver[2],
		"Direccion" :driver[3],
		"Ciudad" :driver[4],
		"Region" : driver[5]
	}


def ns_easy_schema(drivers):
    return [ns_easy(driver) for driver in drivers]

def ns_pendientes_easy_region(driver):
    return {
		"Region" : driver[0],
		"Comuna" : driver[1],
		"Pendiente" : driver[2],
	}

def ns_pendientes_easy_region_schema(drivers):
    return [ns_pendientes_easy_region(driver) for driver in drivers]


def panel_principal_ns_easy(easy):
    return {
		"Total_entregas" : easy[0],
		"Total_entregados" : easy[1],
		"Entregados_hoy" : easy[2],
		"En_ruta" : easy[3],
		"Sin_ruta_beetrack" : easy[4],
		"Anulados" : easy[5],
		"Porcentaje_entrega" : easy[6],
		"Porcentaje_no_entrega" : easy[7],
        "Proyeccion" : easy[8]
	}

def panel_regiones_ns_easy(easy):
    return{
        "Region": easy[0],
        "Total_region": easy[1],
        "Entregados": easy[2],
        "Ns_region": easy[3],
    }


def panel_regiones_ns_easy_schema(easys):
    return [panel_regiones_ns_easy(easy) for easy in easys]



def panel_noentregas_easy(easy):
    return{
        "Estado": easy[0],
        "Total": easy[1],
        "Porcentaje": easy[2],

    }


def panel_noentregas_easy_schema(easys):
    return [panel_noentregas_easy(easy) for easy in easys]
