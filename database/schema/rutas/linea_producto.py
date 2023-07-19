def recuperar_linea_producto_schema(results):
    json_data = []
    keys = ["Cliente", "Ingreso_cliente", "Recepcion", "Planificacion","en_ruta","Producto_entregado"]
    values = results[0][0].split(",")
    data = {}
    for k, v in zip(keys, values):
        v = v.strip("\"")
        if k == "Cliente" or k == "Producto_entregado":
            v = v.strip('()\"" ')
        data[k] = v
    json_data.append(data)

    return json_data