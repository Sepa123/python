def lista_sucursal(data):
    return{
            "id" :data[0],
            "nombre": data[1],
            "pais": data[2],
            "ciudad": data[3],
            "comuna" : data[4],
            "direccion": data[5],

            }

def lista_sucursa_schema(datos):
    return[lista_sucursal(data) for data in datos
    ]

def lista_departamento(data):
    return{
        "id": data[0],
        "nombre": data[1]
    }
def lista_departamento_schema(datos):
    return[lista_departamento(data) for data in datos
    ]