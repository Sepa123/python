def usuario_transporte(usuario):
    return {
        "Id": usuario[0],
        "Created_at": usuario[1],
        "Id_ingreso_hela": usuario[2],
        "Id_user": usuario[3],
        "Ids_user": usuario[4],
        "Id_razon_social": usuario[5],
        "Jpg_foto_perfil": usuario[6],
        "Nombre_completo": usuario[7],
        "Rut": usuario[8],
        "Nro_serie_cedula": usuario[9],
        "Email": usuario[10],
        "Telefono": usuario[11],
        "Birthday": usuario[12],
        "Region": usuario[13],
        "Comuna": usuario[14],
        "Domicilio": usuario[15],
        "Tipo_usuario": usuario[16],
        "Pdf_antecedentes": usuario[17],
        "Pdf_licencia_conducir": usuario[18],
        "Fec_venc_lic_conducir": usuario[19],
        "Pdf_cedula_identidad": usuario[20],
        "Pdf_contrato": usuario[21],
        "Activo": usuario[22],
        "Validacion_seguridad": usuario[23],
        "Validacion_transporte": usuario[24],
        "Razon_social": usuario[25]
    }

def usuarios_transporte_schema(usuarios):
    return [usuario_transporte(usuario) for usuario in usuarios]