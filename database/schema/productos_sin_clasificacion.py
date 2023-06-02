def producto_sin_clasificacion(producto):
    return {
        "SKU" : producto[0],
        "Descripcion" : producto[1],
        "Talla" : producto[2]
    }

def productos_sin_clasificacion_schema(productos):
    return [producto_sin_clasificacion(producto) for producto in productos]