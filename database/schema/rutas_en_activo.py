def ruta_en_activo_schema(ruta):
    return {
            "Pos" : ruta[0],
            "Codigo_pedido":ruta[1],
            "Comuna": ruta[2],
            "SKU": ruta[3],
            "Producto": ruta[4],
            "Unidades": ruta[5],
            "Bultos": ruta[6],
            "Nombre_cliente": ruta[7],
            "Direccion_cliente": ruta[8],
            "Telefono": ruta[9],
            "Validacion": ruta[10],
            "DE": ruta[11],
            "DP": ruta[12]
        }

def rutas_en_activo_schema(rutas): 
    return [ruta_en_activo_schema(ruta) for ruta in rutas]