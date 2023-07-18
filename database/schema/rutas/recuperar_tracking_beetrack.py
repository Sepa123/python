# def recuperar_tracking_beetrack(beetrack):
#     return {
#   "Id Ruta": beetrack[1],
#   "PPU": null,
#   "Fecha Ingreso Beetrack": null,
#   "Cliente": null,
#   "Fecha Entrega": null,
#   "Estado": null,
#   "Subestado": null
# }


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





