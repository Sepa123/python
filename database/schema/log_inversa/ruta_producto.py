def ruta_producto(ruta):
    return {
        "Ruta_beetrack": ruta[0],
        "Ruta_ty": ruta[1],
        "Patente": ruta[2],
        "Driver": ruta[3],
    }


def ruta_productos_schema(rutas):
    return [ ruta_producto(ruta) for ruta in rutas]