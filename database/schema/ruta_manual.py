def convert_to_json(results):
    json_data = []
    keys = ['Codigo_cliente', 'Nombre', 'Calle', 'Direccion_textual', 'Ciudad', 'Provincia', 'Latitud', 'Longitud', 'Telefono', 'Email', 'Codigo_pedido', 'Fecha_pedido', 'Operacion', 'Codigo_producto',
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



def ruta_manual(ruta):

    return {
        "Codigo_cliente": ruta[0],
        "Nombre":  ruta[1],
        "Calle": ruta[2],
        "Direccion_textual": ruta[3],
        "Ciudad": ruta[4],
        "Provincia": ruta[5],
        "Latitud": ruta[6],
        "Longitud": ruta[7],
        "Telefono": ruta[8],
        "Email": ruta[9],
        "Codigo_pedido": ruta[10],
        "Fecha_pedido": ruta[11],
        "Operacion": ruta[12],
        "Codigo_producto": ruta[13],
        "Descripcion_producto": ruta[14],
        "Cantidad_producto": ruta[15],
        "Peso": ruta[16],
        "Volumen": ruta[17],
        "Dinero": ruta[18],
        "Duracion_min": ruta[19],
        "Ventana_horaria_1": ruta[20],
        "Ventana_horaria_2": ruta[21],
        "Notas": ruta[22],
        "Agrupador": ruta[23],
        "Email_remitentes": ruta[24],
        "Eliminar_pedido": ruta[25],
        "Vehiculo": ruta[26],
        "Habilidades": ruta[27],
        "SKU": ruta[28],
        "Pistoleado": ruta[29],
        "Tamaño": ruta[30],
        "Estado": ruta[31],
        "En_ruta": ruta[32],
        "TOC" : ruta[33],
        "Obs_TOC": ruta[34],
        "Sistema": ruta[35],
        "Obs_sistema": ruta[36]
    }


def rutas_manuales_schema(rutas):
    return [ruta_manual(ruta) for ruta in rutas]