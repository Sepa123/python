def lista_persona_equipo_asignado(data):
    return{
            "id": data[0],
            "persona": data[1],
            "departamento":data[2],
            "equipo": data[3],
            "folio_entrega":data[4],
            "fecha_entrega":data[5],
            "firma_entrega":data[6],
            "pdf_entrega":data[7],
            "folio_devolucion":data[8],
            "fecha_devolucion": data[9],
            "firma_devolucion": data[10],
            "pdf_devolucion":data[11],
            "estado":data[12],
            "observacion":data[13],
            "tipo": data[14]
            }

def persona_equipo_schema(datos):
    return[lista_persona_equipo_asignado(data) for data in datos
    ]
def lista_accesorio_asignado(data):
    return{
        "id": data[0],
        "departamento":data[1],
        "equipo": data[2],
        "persona":data[3],
        "fecha_entrega": data[4],
        "estado":data[5],
        "observacion":data[6],
        "id_asignacion":data[7]
    }
def accesorio_schema(datos):
    return[lista_accesorio_asignado(data) for data in datos]

def lista_insumos_asignado(data):
    return{
        "id": data[0],
        "departamento":data[1],
        "equipo": data[2],
        "fecha_entrega": data[3],
        "estado":data[4],
        "observacion":data[5],
        "id_asignacion":data[6]
    }
def insumo_schema(datos):
    return[lista_insumos_asignado(data) for data in datos]

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
            "comuna": data[11],
            "banco":data[12],
            "tipo_cuenta":data[13],
            "numero_cuenta":data[14],
            "correo":data[15],
            "afp":data[16],
            "salud":data[17],
            "telefono_adicional":data[18],
            "nombre_contacto":data[19],
            "seguro_covid":data[20],
            "horario":data[21],
            "ceco":data[22],
            "sueldo_base":data[23],
            "tipo_contrato":data[24],
            "direccion_laboral":data[25],
            "enfermedad":data[26],
            "polera":data[27],
            "pantalon":data[28],
            "poleron":data[29],
            "zapato":data[30],
            "foto":data[31],
            "pdf":data[32],
            "req_comp":data[33],
            "req_cel":data[34],
            "habilitado":data[35]

            }

def crear_persona_schema(datos):
    return[lista_crear_persona(data) for data in datos]

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
            "equipo_id":data[12],
            "folio_devolucion":data[13],
            "descripcion": data[14],
            "almacenamiento": data[15],
            "ram":data[16],
            "tipo":data[17],
            "firma_entrega": data[18],
            "firma_devolucion": data[19],
            "pdf_entrega":data[20],
            "pdf_devolucion":data[21]
            # "modelo":data[17]


            }

def equipo_asignado_por_id_schema(datos):
    return[lista_equipo_asignado_por_id(data) for data in datos]

def lista_equipo_devolucion_por_id(data):
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
            "folio_devolucion": data[9],
            "fecha_devolucion":data[10],
            "estado":data[11],
            "equipo_id":data[12]

            }

def equipo_devuelto_por_id_schema(datos):
    return[lista_equipo_devolucion_por_id(data) for data in datos]


def lista_equipo_asignado_por_persona(data):
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
            "descripcion": data[11],
            "almacenamiento": data[12],
            "ram": data[13],
            "equipo_id": data[14],
            "tipo": data[15],
            "estado":data[16],
            }

def equipo_asignado_por_persona_schema(datos):
    return[lista_equipo_asignado_por_persona(data) for data in datos]


def lista_todos_los_equipo_asignado_por_persona(data):
    return{
            "id": data[0],
            "persona": data[1],
            "rut":data[2],
            "cargo": data[3],
            "departamento":data[4],
            "marca":data[5],
            "serial": data[6],
            "equipo":data[7],
            "fecha_entrega":data[8],
            "descripcion": data[9],
            "equipo_id": data[10],
            "tipo": data[11],
            "estado":data[12],
            }

def todos_los_equipo_asignado_por_persona_schema(datos):
    return[lista_todos_los_equipo_asignado_por_persona(data) for data in datos]

def equipos_asignado_por_serial(data):
    return{
            "id": data[0],
            "persona": data[1],
            "tipo":data[2],
             "marca":data[3],
             "modelo":data[4],
            "serial": data[5],
             "estado":data[6],
            "subestado": data[7],
            "fecha_entrega":data[8],
            "fecha_devolucion":data[9]
            }

def equipos_asignado_por_serial_schema(datos):
    return[equipos_asignado_por_serial(data) for data in datos]

def ruta_pdf_entrega(data):
    return{
            "id": data[0],
            "pdf_entrega":data[1],
            
    }

def ruta_pdf_entrega_schema(datos):
    return[ruta_pdf_entrega(data) for data in datos
    ]


def ruta_pdf_devolucion_schema(result):
    return result

def ultima_persona_schema(result):
    return result

def firma_entrega(data):
    return {
        "id": data[0],
        "firma_entrega": data[1],
        "pdf_entrega":data[2],
            
    }

def firma_entrega_schema(datos):
    return[firma_entrega(data) for data in datos
    ]

def firma_devolucion(data):
    return {
        "id": data[0],
        "firma_devolucion": data[1],
        "pdf_devolucion":data[2],
            
    }

def firma_devolucion_schema(datos):
    return[firma_devolucion(data) for data in datos]

# def ruta_pdf_devolucion(data):
#     return{
#             "id": data[0],
#             "pdf_devolucion":data[0],
            
#     }

# def ruta_pdf_devolucion_schema(datos):
#     return[ruta_pdf_devolucion(data) for data in datos
#     ]

