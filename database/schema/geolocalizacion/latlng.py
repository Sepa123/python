def latlng (ruta) :
    return {
    "Id_usuario" : ruta[0],
    "Direccion" : ruta[1],
    "Comuna" : ruta[2],
    "Region" : ruta[3],
    "Lat" : ruta[4],
    "Lng" : ruta[5],
    "Ids_usuario" : ruta[6],
    "Display_name" : ruta[7],
    "Type" : ruta[8]
}


def latlng_schema(rutas):
    return [latlng(ruta) for ruta in rutas]