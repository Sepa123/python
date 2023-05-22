def pedido_tiendas_easy_opl_schema(pedido):
    return {
        "Tiendas": pedido[0],
        "Productos": pedido[1]
    }

def pedidos_tiendas_easy_opl_schema(pedidos):
    return [pedido_tiendas_easy_opl_schema(pedido) for pedido in pedidos]