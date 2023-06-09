def convert_to_json(results):
    json_data = []
    keys = ['Codigo_cliente', 'Nombre', 'Calle', 'Ciudad', 'Provincia', 'Latitud', 'Longitud', 'Telefono', 'Email', 'Codigo_pedido', 'Fecha_pedido', 'Operacion', 'Codigo_producto',
             'Descripcion_producto', 'Cantidad_producto', 'Peso', 'Volumen', 'Dinero', 'Duracion_min', 'Ventana_horaria_1', 'Ventana_horaria_2', 'Notas', 'Agrupador', 
             'Email_remitentes', 'Eliminar_pedido', 'Vehiculo', 'Habilidades','SKU', 'Pistoleado', 'Tamaño', 'Estado', 'En_ruta']
    print(type(results))
    for result in results:
        values = result[0].split(",")
        data = {}
        for k, v in zip(keys, values):
            v = v.strip("\"")
            if k == "Estado" or k == "Codigo_cliente" or k =="En_ruta":
                v = v.strip("()")
            data[k] = v
        # data["RutaEnTabla"] = ""
        json_data.append(data)

    return json_data