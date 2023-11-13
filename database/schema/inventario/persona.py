def lista_persona_equipo_asignado(data):
    return{
            "id": data[0],
            "persona": data[1],
            "departamento":data[2],
            "nombre_equipo":data[3],
            "equipo": data[4],
            "folio":data[5],
            "fecha_entrega":data[6],
            "fecha_devolucion": data[7],
            "estado":data[8],
            "observacion":data[9],
            }

def persona_equipo_schema(datos):
    return[lista_persona_equipo_asignado(data) for data in datos
    ]

def lista_crear_persona(data):
    return{
            "id" :data[0],
            "nombres": data[1],
            "apellidos":data[2],
            "rut":data[3],
            "nacionalidad":data[4],
            "fecha_nacimiento":data[5],
            "estado_civil":data[6],
            "telefono":data[7],
            "fecha_ingreso":data[8],
            "cargo":data[9],
            "domicilio":data[10],
            "banco":data[11],
            "tipo_cuenta":data[12],
            "numero_cuenta":data[13],
            "correo":data[14],
            "afp":data[15],
            "salud":data[16],
            "telefono_adicional":data[17],
            "nombre_contacto":data[18],
            "seguro_covid":data[19],
            "horario":data[20],
            "ceco":data[21],
            "sueldo_base":data[22],
            "tipo_contrato":data[23],
            "direccion_laboral":data[24],
            "enfermedad":data[25],
            "polera":data[26],
            "pantalon":data[27],
            "poleron":data[28],
            "zapato":data[29]
            }

def crear_persona_schema(datos):
    return[lista_crear_persona(data) for data in datos
    ]

def lista_equipo_asignado_por_id(data):
    return{
            "id": data[0],
            "nombres": data[1],
            "apellidos":data[2],
            "rut":data[3],
            "cargo": data[4],
            "departamento":data[5],
            "marca":data[6],
            "serial": data[7],
            "equipo":data[8],
            "folio_entrega": data[9],
            "fecha_entrega":data[10],
            "estado":data[11],
            }

def equipo_asignado_por_id_schema(datos):
    return[lista_equipo_asignado_por_id(data) for data in datos
    ]