def colaborador(colab):
    return {
                "Id": colab[0],
                "Created_at": colab[1],
                "Id_user": colab[2],
                "Ids_user": colab[3],
                "Date_modified": colab[4],
                "Tipo_razon": colab[5],
                "Razon_social": colab[6],
                "Rut": colab[7],
                "Celular": colab[8],
                "Telefono": colab[9],
                "Region": colab[10],
                "Comuna": colab[11],
                "Direccion": colab[12],
                "Representante_legal": colab[13],
                "Rut_representante_legal": colab[14],
                "Email_rep_legal": colab[15],
                "Direccion_comercial": colab[16],
                "Pdf_legal_contitution": colab[17],
                "Pdf_registration_comerce": colab[18],
                "Pdf_validity_of_powers": colab[19],
                "Pdf_certificate_rrpp": colab[20],
                "Chofer": colab[21],
                "Peoneta": colab[22],
                "Abogado": colab[23],
                "Seguridad": colab[24],
                "Activo": colab[25],
                "Giro" : colab[26],
                "Pdf_contrato" : colab[27],
                "Vehiculos": colab[28],
                "Tripulacion": colab[29],
                "Patentes": colab[30],
                "Usuarios" : colab[31],
                "Fecha_nacimiento": colab[32]
            }

def colaboradores_schema(colaboradores):
    return [colaborador(colab) for colab in colaboradores]



def detalle_pago(detalle):
    return {
        "Id": detalle[0],
        "Created_at": detalle[1],
        "Id_user": detalle[2],
        "Ids_user": detalle[3],
        "Id_razon_social": detalle[4],
        "Rut_cuenta": detalle[5],
        "Titular_cuenta": detalle[6],
        "Numero_cuenta": detalle[7],
        "Banco": detalle[8],
        "Email": detalle[9],
        "Tipo_cuenta": detalle[10],
        "Forma_pago": detalle[11],
        "Pdf_documento": detalle[12],
        "Estado": detalle[13]
    }

def detalle_pagos_schema(detalles):
    return [detalle_pago(detalle) for detalle in detalles]




def motivo_desvinculacion(motivo):
    return {
        "id": motivo[0],
        "motivo": motivo[1]

    }

def motivo_desvinculacion_schema(detalles):
    return [motivo_desvinculacion(detalle) for detalle in detalles]
