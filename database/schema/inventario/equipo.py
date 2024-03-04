def lista_tipo_equipo(data):
    return{
            "id": data[0],
            "nombre": data[1],

            
            }

def tipo_equipo_schema(datos):
    return[lista_tipo_equipo(data) for data in datos
    ]

def lista_descripcion_equipo(data):
    return{
      
            "id": data[0],
            "marca": data[1],
            "modelo":data[2],
            "serial":data[3],
            "mac_wifi":data[4],
            "serie":data[5],
            "resolucion":data[6],
            "dimensiones":data[7],
            "descripcion":data[8],
            "ubicacion":data[9],
            "almacenamiento":data[10],
            "ram":data[11],
            "estado":data[12],
            "subestado": data[13],
            "tipo":data[14],
            "cantidad" :data[15],
            "nr_equipo": data[16],
           
            
            }

def descripcion_equipo_schema(datos):
    return[lista_descripcion_equipo(data) for data in datos
    ]

def lista_equipo_sin_join(data):
    return{
      
            "id": data[0],
            "marca": data[1],
            "modelo":data[2],
            "serial":data[3],
            "mac_wifi":data[4],
            "serie":data[5],
            "resolucion":data[6],
            "dimensiones":data[7],
            "descripcion":data[8],
            "ubicacion":data[9],
            "almacenamiento":data[10],
            "ram":data[11],
            "estado":data[12],
            "tipo":data[13],
            "cantidad" :data[14],
            "nr_equipo": data[15],
            "subestado" :data[16]
           
            
            }

def lista_equipo_sin_join_schema(datos):
    return[lista_equipo_sin_join(data) for data in datos
    ]
def lista_equipos_disponibles(data):
    return{
      
            "id": data[0],
            "marca": data[1],
            "modelo":data[2],
            "serial":data[3],
            "mac_wifi":data[4],
            "serie":data[5],
            "resolucion":data[6],
            "dimensiones":data[7],
            "descripcion":data[8],
            "ubicacion":data[9],
            "almacenamiento":data[10],
            "ram":data[11],
            "estado":data[12],
            "tipo":data[13],
            "cantidad" :data[14],
            "nr_equipo": data[15],
            "sub_estado": data[16]
            }

def lista_equipos_disponibles_schema(datos):
    return[lista_equipos_disponibles(data) for data in datos
    ]

def lista_estado(data):
    return{
         "id": data[0],
        "estado": data[1],
        "descripcion": data[2]
    }
def lista_estado_schema(datos):
    return[lista_estado(data) for data in datos ]

def lista_inventario_estado(data):
    return{
        "id": data[0],
        "nombre": data[1]
    }
def lista_inventario_estado_schema(datos):
    return[lista_inventario_estado(data) for data in datos ]

def lista_estado_devolucion(data):
    return{
        "id": data[0],
        "estado": data[1],
        "descripcion": data[2]
    }
def lista_estado_devolucion_schema(datos):
    return [lista_estado_devolucion(data) for data in datos]

def lista_subestado(data):
    return{
        "id": data[0],
        "parent_code": data[1],
        "code":data[2],
        "descripcion": data[3]
    }
def lista_subestado_schema(datos):
    return[lista_subestado(data) for data in datos ]

def lista_licencia(data):
    return{
        "id": data[0],
        "codigo": data[1]
    }

def licencia_equipo_schema(datos):
    return[lista_licencia(data) for data in datos]

def lista_licencia_asignada_equipo(data):
    return{
        "id": data[0],
        "persona": data[1],
        "equipo": data[2],
        "serial": data[3],
        "codigo": data[4],
        "asignado": data[5],
        "id_equipo": data[6],
        "id_persona": data[7]

    }

def lista_chip_asignado_equipo(data):
    return{
        "id_chip": data[0],
        "persona": data[1],
        "linea": data[2],
        "serial": data[3],
        "numero": data[4],
        "id_equipo": data[5],
        "celular": data[6],
        "imei": data[7],
        "estado":data[8]
    }
def lista_chip_asignado_equipo_schema(datos):
    return[lista_chip_asignado_equipo(data) for data in datos]

def lista_licencia_asignada_equipo_schema(datos):
    return[lista_licencia_asignada_equipo(data) for data in datos]


def folio_entrega_schema(result):
    return result

def folio_devolucion_schema(result):
    return result


def lista_nr_equipo_schema(result):
     return result

def lista_nr_code_schema(result):
    return result

def ultimo_estado_schema(result):
    return result

def ultimo_equipo_schema(result):
    return result