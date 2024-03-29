def producto_sku(producto):
    return {
        "Codigo_cliente": producto[0],
        "Nombre": producto[1],
        "Calle": producto[2],
        "Ciudad": producto[3],
        "Provincia": producto[4],
        "Latitud": producto[5],
        "Longitud": producto[6],
        "Telefono": producto[7],
        "Email": producto[8],
        "Codigo_pedido": producto[9],
        "Fecha_pedido": producto[10],
        "Operacion": producto[11],
        "Codigo_producto": producto[12],
        "Descripcion_producto": producto[13],
        "Cantidad_producto": producto[14],
        "Peso": producto[15],
        "Volumen": producto[16],
        "Dinero": producto[17],
        "Duracion_min": producto[18],
        "Ventana_horaria_1": producto[19],
        "Ventana_horaria_2": producto[20],
        "Notas": producto[21],
        "Agrupador": producto[22],
        "Email_remitentes": producto[23],
        "Eliminar_pedido": producto[24],
        "Vehiculo": producto[25],
        "Habilidades": producto[26],
        "SKU": producto[27],
        "Pistoleado": producto[28],
        "Tamaño": producto[29],
        "Estado": producto[30],
        "En_ruta": producto[31]
    }

def productos_sku_schema(productos):
    return [producto_sku(producto) for producto in productos]