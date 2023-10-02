def generar_codigo_ty_nota_venta_rsv(numero):
    # Convierte el número en una cadena y rellena con ceros a la izquierda
    numero_formateado = str(numero).zfill(5)
    # Concatena el prefijo con el número formateado
    codigo = "TYRSV" + numero_formateado
    return codigo