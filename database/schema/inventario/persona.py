def lista_persona_equipo_asignado(data):
    return{
            "persona": data[0],
            "departamento":data[1],
            "nombre_equipo":data[2],
            "equipo": data[3],
            "folio":data[4],
            "fecha_entrega":data[5],
            "fecha_devolucion": data[6],
            "estado":data[7],
            "observacion":data[8],
            
            
            
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