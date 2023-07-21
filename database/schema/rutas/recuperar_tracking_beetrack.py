
def recuperar_tracking_beetrack_schema(results):
    json_data = []
    keys = ["Id_ruta", "PPU", "Fecha_ingreso_beetrack", "Cliente", "Fecha_entrega", "Estado", "Subestado"]
    for result in results:
        values = result[0].split(",")
        data = {}
        for k, v in zip(keys, values):
            v = v.strip("\"")
            if k == "Id_ruta" or k == "Subestado":
                v = v.strip('()\"" ')
            data[k] = v
        json_data.append(data)

    return json_data


def recuperar_fecha_ingreso_sistema_schema(fecha):
    return {
        'Fecha_ingreso_sistema': fecha[0][0]
            }


