def pedido_schema(pedido):
    return {
        "Total_pedidos": pedido[0],
        "Entregados": pedido[1],
        "No_entregados": pedido[2],
        "Pendientes": pedido[3],
    }


def pedidos_schema(pedidos):
    return [pedido_schema(pedido) for pedido in pedidos]