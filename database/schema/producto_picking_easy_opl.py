def producto_picking_easy_opl(producto):
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

def productos_picking_easy_opl_schema(productos):
    return [producto_picking_easy_opl(producto) for producto in productos]