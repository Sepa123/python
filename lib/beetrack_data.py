def obtener_datos_tags(tags):
    nombres_a_buscar = ["conductor", "CMN", "Bultos","Bultos ","EMAIL","Fechaentrega","fechahr",
                        "Volumen","factura","oc","ruta","tienda","codigo","observacion"]

    data = {
        "conductor" : "",
        "CMN" : "",
        "Bultos" : "",
        "EMAIL": "",
        "Fechaentrega": "",
        "fechahr": "",
        "Volumen" : "",
        "factura" : "",
        "oc": "",
        "ruta": "",
        "tienda": "",
        "codigo": "",
        "observacion": "",

    }
    # Buscar los objetos JSON con los nombres especificados
    objetos_json = [item for item in tags if item["name"] in nombres_a_buscar]

    # Obtener los valores asociados con los nombres especificados
    valores = {item["name"]: item["value"] for item in objetos_json}

    # Imprimir los valores encontrados
    for nombre, valor in valores.items():
        data[nombre.strip()] = valor
        # print(f"El valor asociado con '{nombre}' es: {valor}")

    return data

def obtener_datos_groups(groups):
    nombres_a_buscar = [276, 8404, 8405]

    data = {
        "Cliente" : "",
        "Región de despacho" : "",
        "Servicio" : ""
    }
    # Buscar los objetos JSON con los nombres especificados
    objetos_json = [item for item in groups if item["group_category_id"] in nombres_a_buscar]

    # Obtener los valores asociados con los nombres especificados
    valores = {item["group_category"]: item["name"] for item in objetos_json}

    # Imprimir los valores encontrados
    for nombre, valor in valores.items():
        data[nombre.strip()] = valor
        # print(f"El valor asociado con '{nombre}' es: {valor}")

    return data

def generar_data_insert_ruta_transyanez(data,datos_tags,groups):

    return {
            "route_id": data["route_id"],
            "identifier": data["truck_identifier"],
            "guide": data["guide"],
            "Cliente": groups["Cliente"],
            "Servicio":groups["Servicio"],
            "Región de despacho": groups["Región de despacho"],
            "estimated_at": data["estimated_at"],
            "substatus": data["substatus"],
            "driver":  datos_tags["conductor"],
            "contact_identifier": data["contact_identifier"],
            "contact_name": data["contact_name"],
            "contact_address": data["contact_address"],
            "contact_phone": data["contact_phone"],
            "contact_email": data["contact_email"],
            "fechahr": datos_tags["fechahr"],
            "fechaentrega": datos_tags["Fechaentrega"],
            "comuna" : datos_tags["CMN"],
            "volumen": datos_tags["Volumen"],
            "bultos" : datos_tags["Bultos"],
            "factura": datos_tags["factura"],
            "oc": datos_tags["oc"],
            "ruta": datos_tags["ruta"],
            "tienda": datos_tags["tienda"],
            "codigo":datos_tags["codigo"],
            "observacion": datos_tags["observacion"],
            "arrived_at": data["arrived_at"]
            }




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
    
def generar_data_insert_creacion_ruta(data):
    return {
            "resource": data["resource"],
            "event":  data["event"],
            "account_name":  data["account_name"],
            "route": data["route"],
            "account_id": data["account_id"],
            "date": data["date"],
            "truck": data["truck"],
            "truck_driver": data["truck_driver"],
            "started": data["started"],
            "started_at": data["started_at"],
            "ended": data["ended"],
            "ended_at": data["ended_at"]
            }