def recuperar_linea_producto_schema(results):
    json_data = []
    lineas = []
    values = results[0][0].split(",")

    print(values)
    for i in range(1,6):
        lineas.append(values[i].strip('()\"" '))
    data = {
        "Cliente" : values[0].strip('()\"" '),
        "Linea": lineas
    }

    return data