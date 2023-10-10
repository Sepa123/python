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
            "Estado" : ruta[10],
            "Validacion": ruta[11],
            "DE": ruta[12],
            "DP": ruta[13],
            "Region": ruta[14],
            "Fecha_pedido" : ruta[15]
        }

def rutas_en_activo_schema(rutas): 
    return [ruta_en_activo_schema(ruta) for ruta in rutas]



def ruta_en_activo_excel(ruta):
    return {
            "Pos" : ruta[0],
            "Codigo_pedido":ruta[1],
            "Comuna": ruta[2],
            "Nombre_cliente": ruta[3],
            "Direccion_cliente": ruta[4],
            "Telefono": ruta[5],
            "SKU": ruta[6],
            "Producto": ruta[7],
            "Unidades": ruta[8],
            "Bultos": ruta[9],
            "DE" : ruta[10],
            "DP": ruta[11],
            "Fecha_pedido": ruta[12]
        }

def ruta_en_activo_excel_schema(rutas): 
    return [ruta_en_activo_excel(ruta) for ruta in rutas]