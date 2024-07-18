def prefactura_meli(prefactura):
    return {
		"Id_usuario" : prefactura[0],
		"Ids_usuario" : prefactura[1],
		"Id_prefactura" : prefactura[2],
		"Periodo" : prefactura[3],
		"Descripcion" : prefactura[4],
		"Id_de_ruta" : prefactura[5],
		"Fecha_inicio" : prefactura[6],
		"Fecha_fin" : prefactura[7],
		"Patente" : prefactura[8],
		"Id_patente" : prefactura[9],
		"Conductor" : prefactura[10],
		"Cantidad" : prefactura[11],
		"Precio_unitario" : prefactura[12],
		"Descuento" : prefactura[13],
		"Total" : prefactura[14]
	}

def prefactura_meli_schema(prefacturas):
    return [prefactura_meli(prefactura) for prefactura in prefacturas ]