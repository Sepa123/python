def pedido_planificado(pedido):
    return {
        "Id": pedido[0],
        "Created_at": pedido[1],
        "Cod_cliente": pedido[2],
        "Razon_social": pedido[3],
        "Domicilio": pedido[4],
        "Tipo_cliente": pedido[5],
        "Fecha_reparto": pedido[6],
        "Cod_reparto": pedido[7],
        "Maquina": pedido[8],
        "Chofer": pedido[9],
        "Fecha_pedido": pedido[10],
        "Cod_pedido": pedido[11],
        "Cod_producto": pedido[12],
        "Producto": pedido[13],
        "Cantidad": pedido[14],
        "Horario": pedido[15],
        "Arribo": pedido[16],
        "Partida": pedido[17],
        "Peso": pedido[18],
        "Volumen": pedido[19],
        "Dinero": pedido[20],
        "Id_ruta_ty": pedido[21],
        "Id_ruta_beetrack": pedido[22],
        "Observacion": pedido[23],
        "Posicion": pedido[24]
    }


def pedidos_planificados_schema(pedidos):
    return [pedido_planificado(pedido) for pedido in pedidos]