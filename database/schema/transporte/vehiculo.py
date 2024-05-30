def vehiculo(vehiculo):
    return {
            "Id": vehiculo[0],
            "Created_at": vehiculo[1],
            "Update_date": vehiculo[2],
            "Razon_id": vehiculo[3],
            "Ppu": vehiculo[4],
            "Marca": vehiculo[5],
            "Tipo":vehiculo[6],
            "Modelo": vehiculo[7],
            "Ano": vehiculo[8],
            "Region": vehiculo[9],
            "Comuna": vehiculo[10],
            "Estado": vehiculo[11],
            "Activation_date": vehiculo[12],
            "Capacidad_carga_kg": vehiculo[13],
            "Capacidad_carga_m3": vehiculo[14],
            "Platform_load_capacity_kg": vehiculo[15],
            "Crane_load_capacity_kg": vehiculo[16],
            "Permiso_circulacion_fec_venc": vehiculo[17],
            "Soap_fec_venc":  vehiculo[18],
            "Revision_tecnica_fec_venc":  vehiculo[19],
            "Registration_certificate":  vehiculo[20],
            "Pdf_revision_tecnica":  vehiculo[21],
            "Pdf_soap":  vehiculo[22],
            "Pdf_padron":  vehiculo[23],
            "Pdf_gases_certification":  vehiculo[24],
            "Validado_por_id":  vehiculo[25],
            "Validado_por_ids": vehiculo[26],
            "Razon_social": vehiculo[27],
            "Rut": vehiculo[28],
            "Gps": vehiculo[29],
            "Id_gps": vehiculo[30],
            "Imei": vehiculo[31],
            "Fecha_instalacion": vehiculo[32],
            "Oc_instalacion": vehiculo[33],
            "Hab_seguridad" : vehiculo[34],
            "Hab_vehiculo" : vehiculo[35]
        }

def vehiculos_schema(colaboradores):
    return [vehiculo(colab) for colab in colaboradores]


def operacion_vehiculo(ope):
    return {
                "Id" : ope[0],
                "Created_at" : ope[1],
                "Id_user" : ope[2],
                "Ids_user" : ope[3],
                "Id_ppu" : ope[4],
                "Id_operacion" : ope[5],
                "Id_centro_op" : ope[6],
                "Estado" : ope[7]
            }

def operacion_vehiculo_schema(operacion):
    return [operacion_vehiculo(ope) for ope in operacion]