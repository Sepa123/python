def pedido_sin_tienda_schema(pedido):
    return {
        "Suborden" : pedido[0],
        "Id_entrega" : pedido[1],
        "Descripcion" : pedido[2],
        "Unidades" : pedido[3],
        "Fecha_compromiso" : pedido[4],
    }

def pedidos_sin_tienda_schema(pedidos):
    return [pedido_sin_tienda_schema(pedido) for pedido in pedidos] 