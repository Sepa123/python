def datos_ruta_activa_editar(ruta):
   datos_extra = ruta[25].split('@')
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
        "Posicion" : ruta[19],
        "Fecha_ruta": ruta[20],
        "DE" : ruta[21],
        "DP" : ruta[22],
        "TOC" : ruta[23],
        "Obs_TOC": ruta[24],
        "Obs_sistema": datos_extra[1],
        "Sistema" : int(datos_extra[0]),
        "Pistoleado": int(datos_extra[2]),
        "En_ruta": int(datos_extra[3]),
        "Estado_entrega" : datos_extra[4],
        "Alerta_conductor": ruta[26],
        "Fecha_original_pedido": ruta[27]
}

def datos_rutas_activas_editar_schema(rutas):
    return [datos_ruta_activa_editar(ruta) for ruta in rutas]