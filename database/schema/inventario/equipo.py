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
            "tipo":data[13],
            "cantidad" :data[14],
            "nr_equipo": data[15]
            }

def descripcion_equipo_schema(datos):
    return[lista_descripcion_equipo(data) for data in datos
    ]

def lista_inventario_estado(data):
    return{
        "id": data[0],
        "nombre": data[1]
    }
def lista_inventario_estado_schema(datos):
    return[lista_inventario_estado(data) for data in datos
    ]

def lista_licencia(data):
    return{
        "id": data[0],
        "codigo": data[1]
    }

def licencia_equipo_schema(datos):
    return[lista_licencia(data) for data in datos]