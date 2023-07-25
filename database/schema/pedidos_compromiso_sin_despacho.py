def pedido_compromiso_sin_despacho_schema(pedido_compromiso):
    return {
        "Origen": pedido_compromiso[0],
        "Cod_entrega": pedido_compromiso[1],
        "Fecha_ingreso": pedido_compromiso[2],
        "Fecha_compromiso": pedido_compromiso[3],
        "Region": pedido_compromiso[4],
        "Comuna": pedido_compromiso[5],
        "Descripcion": pedido_compromiso[6],
        "Bultos": pedido_compromiso[7],
        "Estado": pedido_compromiso[8],
        "Subestado": pedido_compromiso[9],
        "Verificado": pedido_compromiso[10],
        "Recibido": pedido_compromiso[11]
    }

def pedidos_compromiso_sin_despacho_schema(pedidos_compromiso):
    return [pedido_compromiso_sin_despacho_schema(pedido_compromiso) for pedido_compromiso in pedidos_compromiso]