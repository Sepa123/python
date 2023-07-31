def datos_ruta_activa_editar(ruta):
   return {
        "Id_ruta": ruta[0],
        "Nombre_ruta": ruta[1],
        "Codigo_cliente": ruta[2],
        "Nombre": ruta[3],
        "Calle": ruta[4],
        "Ciudad":ruta[5],
        "Provincia": ruta[6],
        "Telefono": ruta[7],
        "Email": ruta[8],
        "Codigo_pedido": ruta[9],
        "Fecha_pedido": ruta[10],
        "Codigo_producto": ruta[11],
        "Descripcion_producto": ruta[12],
        "Cantidad_producto": ruta[13],
        "Notas": ruta[14],
        "Agrupador": ruta[15],
        "SKU": ruta[16],
        "Tamaño": ruta[17],
        "Estado": ruta[18],
        "Posicion" : ruta[19]
}

def datos_rutas_activas_editar_schema(rutas):
    return [datos_ruta_activa_editar(ruta) for ruta in rutas]