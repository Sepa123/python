def region_lista (region):
    return {
        "Id_region": region[0],
        "Region_num": region[1],
        "Nombre_region": region[2],
    }

def regiones_schema(regiones):
    region_list = [region_lista(region) for region in regiones]
    region_list.append(
        {
            "Id_region": 0,
            "Region_num":' ',
            "Nombre_region":'S/I',
        }
    )
    return region_list


# SELECT id_comuna, comuna_name, id_region
# FROM public.op_comunas
# where id_region = '1'

def comuna_lista(comuna):
    return {
        "Nombre_comuna": comuna[0],
        "Id_region": comuna[1],
        "Id_comuna" : comuna[2]
    }

def comunas_lista_schema(comunas):
    comunas_list = [comuna_lista(comuna) for comuna in comunas]
    comunas_list.append({
        "Nombre_comuna": 'S/I',
        "Id_region": 0,
        "Id_comuna" : 0
    })
    return comunas_list