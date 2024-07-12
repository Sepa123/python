def prefactura_meli(prefactura):
    return {
		"Id" : prefactura[0],
		"Created_at" : prefactura[1],
		"Id_usuario" : prefactura[2],
		"Ids_usuario" : prefactura[3],
		"Id_prefactura" : prefactura[4],
		"Periodo" : prefactura[5],
		"Descripcion" : prefactura[6],
		"Id_de_ruta" : prefactura[7],
		"Fecha_de_inicio" : prefactura[8],
		"Fecha_de_fin" : prefactura[9],
		"Patente" : prefactura[10],
		"Conductor" : prefactura[11],
		"Cantidad" : prefactura[12],
		"Precio_unitario" : prefactura[13],
		"Total" :prefactura[14]
	}

def prefactura_meli_schema(prefacturas):
    return [prefactura_meli(prefactura) for prefactura in prefacturas ]