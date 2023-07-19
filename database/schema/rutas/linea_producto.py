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
    # print(values)
    # data = {}
    # for k, v in zip(keys, values):
    #     v = v.strip("\"")
    #     if k == "Cliente" or k == "Producto_entregado":
    #         v = v.strip('()\"" ')
    #     data[k] = v
    # json_data.append(data)

    return data