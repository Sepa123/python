def notificacion(noti):
    return {
        "Numero_Factura": noti[0],
        "Visto" : False
    }

def notificaciones_schema(notis):
    return [notificacion(noti) for noti in notis]