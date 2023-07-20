def region_lista (region):
    return {
        "Id_region": region[0],
        "Region_num": region[1],
        "Nombre_region": region[2],
    }

def regiones_schema(regiones):
    return [region_lista(region) for region in regiones]