def factura_electrolux(factura):
    return {
         factura[0]:factura[1]
    }

def facturas_electrolux_schema(facturas):
    return [factura_electrolux(factura) for factura in facturas]