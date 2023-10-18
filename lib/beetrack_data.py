def obtener_datos_tags(tags):
    nombres_a_buscar = ["conductor", "CMN", "Bultos","Bultos " ]

    data = {}
    # Buscar los objetos JSON con los nombres especificados
    objetos_json = [item for item in tags if item["name"] in nombres_a_buscar]

    # Obtener los valores asociados con los nombres especificados
    valores = {item["name"]: item["value"] for item in objetos_json}

    # Imprimir los valores encontrados
    for nombre, valor in valores.items():
        data[nombre.strip()] = valor
        # print(f"El valor asociado con '{nombre}' es: {valor}")

    return data


def generar_data_insert(data,item,datos_tags,waypoint):
    return {
        "resource": data["resource"],
        "event":  data["event"],
        "identifier": data["identifier"],
        "truck_identifier": data["truck_identifier"],
        "status": data["status"],
        "substatus":  data["substatus"],
        "substatus_code":  data["substatus_code"],
        "contact_identifier": data["contact_identifier"],
        "bultos" : datos_tags["Bultos"],
        "comuna" : datos_tags["CMN"],
        "driver" : datos_tags["conductor"],
        "item_name":  item["name"],
        "item_quantity":  item["quantity"],
        "item_delivered_quantity" : item["delivered_quantity"],
        "item_code" : item["code"],
        "arrived_at": data["arrived_at"],
        "latitude": str(waypoint["latitude"]),
        "longitude": str(waypoint["longitude"])
        }
    
