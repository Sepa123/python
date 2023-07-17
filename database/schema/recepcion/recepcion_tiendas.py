def recepcion_tienda(producto):
    return {"Codigo_cliente": producto[0],
            "Nombre": producto[1],
            "Calle": producto[2],
            # "Ciudad": producto[3],
            "Provincia": producto[3],
            "Codigo_pedido": producto[4],
            "Fecha_pedido":producto[5],
            "Codigo_producto":producto[6],
            "Descripcion_producto":producto[7],
            "Cantidad_producto":producto[8],
            "SKU":producto[9],
            "Pistoleado":producto[10]
            }

def recepcion_tiendas_schema(productos):
    return [recepcion_tienda(producto) for producto in productos]

def recepcion_easy_cd(producto):
    return {"Codigo_cliente": producto[0],
            "Nombre": producto[1],
            "Calle": producto[2],
            "Provincia": producto[3],
            "Codigo_pedido": producto[4],
            "Fecha_pedido":producto[5],
            "Codigo_producto":producto[6],
            "Descripcion_producto":producto[7],
            "Cantidad_producto":producto[8],
            "SKU":producto[9],
            "Pistoleado":producto[10],
            "Carga" : producto[11],
            "Recepcion" : producto[12]
            }


def recepcion_easy_cds_schema(productos):
    return [recepcion_easy_cd(producto) for producto in productos]