def carga_quadminds(quadminds):
    return {
	"Codigo_cliente": quadminds[0],
	"Nombre": quadminds[1],
	"Calle": quadminds[2],
	"Ciudad": quadminds[3],
	"Provincia": quadminds[4],
	"Latitud": quadminds[5],
	"Longitud": quadminds[6],
	"Telefono": quadminds[7],
	"Email": quadminds[8],
	"Codigo_pedido": quadminds[9],
	"Fecha_pedido": quadminds[10],
	"Operacion": quadminds[11],
	"Codigo_producto": quadminds[12],
	"Descripcion_producto": quadminds[13],
	"Cantidad_producto": quadminds[14],
	"Peso": quadminds[15],
	"Volumen": quadminds[16],
	"Dinero": quadminds[17],
	"Duracion_min": quadminds[18],
	"Ventana_horaria_1": quadminds[19],
	"Ventana_horaria_2": quadminds[20],
	"Notas": quadminds[21],
	"Agrupador": quadminds[22],
	"Email_remitentes": quadminds[23],
	"Eliminar_pedido": quadminds[24],
	"Vehiculo": quadminds[25],
	"Habilidades": quadminds[26]
}

def cargas_quadminds_schema(cquadminds):
    return [ carga_quadminds(quadminds) for quadminds in cquadminds]