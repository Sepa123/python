def cargas_easy_cd(carga):
    return {
            "id": carga[0],
            "created_at": carga[1],
            "anden": carga[2],
            "cd": carga[3],
            "nro_carga": carga[4],
            "entrega": carga[5],
            "fecha_entrega": carga[6],
            "tipo_orden": carga[7],
            "carton": carga[8],
            "comuna": carga[9],
            "region": carga[10],
            "producto": carga[11],
            "descripcion": carga[12],
            "unid": carga[13],
            "bultos": carga[14],
            "nombre": carga[15],
            "direccion": carga[16],
            "telefono": carga[17],
            "correo": carga[18],
            "compl": carga[19],
            "cant": carga[20],
            "verified": carga[21],
            "estado": carga[22],
            "subestado": carga[23],
            "recepcion": carga[24]
            }

def carga_easy_cd_schema(cargas):
    return [cargas_easy_cd(carga) for carga in cargas]