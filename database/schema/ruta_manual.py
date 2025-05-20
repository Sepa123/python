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
        "Fecha_original_pedido" : ruta[12],
        "Operacion": ruta[13],
        "Codigo_producto": ruta[14],
        "Descripcion_producto": ruta[15],
        "Cantidad_producto": ruta[16],
        "Peso": ruta[17],
        "Volumen": ruta[18],
        "Dinero": ruta[19],
        "Duracion_min": ruta[20],
        "Ventana_horaria_1": ruta[21],
        "Ventana_horaria_2": ruta[22],
        "Notas": ruta[23],
        "Agrupador": ruta[24],
        "Email_remitentes": ruta[25],
        "Eliminar_pedido": ruta[26],
        "Vehiculo": ruta[27],
        "Habilidades": ruta[28],
        "SKU": ruta[29],
        "Pistoleado": ruta[30],
        "Tamaño": ruta[31],
        "Estado": ruta[32],
        "En_ruta": ruta[33],
        "TOC" : ruta[34],
        "Obs_TOC": ruta[35],
        "Sistema": ruta[36],
        "Obs_sistema": ruta[37],
        "Id_cliente": ruta[38]
    }


def rutas_manuales_schema(rutas):
    return [ruta_manual(ruta) for ruta in rutas]