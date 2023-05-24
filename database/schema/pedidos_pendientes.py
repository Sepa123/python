def pedido_pendiente_schema(pedido):
    return {
        "Atrasadas": pedido[0],
        "En_fecha":  pedido[1],
        "Adelantadas":  pedido[2]
    }


def pedidos_pendientes_schema(pedidos):
    return [pedido_pendiente_schema(pedido) for pedido in pedidos]