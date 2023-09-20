def recepcion_pendiente(producto):
    return{
            "Fecha_Ingreso": producto[0],
            "Cliente": producto[1],
            "Calle": producto[2],
            "Ciudad": producto[3],
            "Codigo_pedido": producto[4],
            "Fecha_pedido":producto[5],
            "Codigo_producto":producto[6],
            "Descripcion_producto":producto[7],
            "Cantidad_producto":producto[8],
            "SKU":producto[9],
            "Pistoleado":producto[10],
            "Recepcion":producto[11]
            }

def recepcion_pendiente_schema(productos):
    return[recepcion_pendiente(producto) for producto in productos
    ]