def ruta_de_pendientes(ruta):
	return {
	"Origen": ruta[0],
	"Cod_entrega": ruta[1],
	"Fecha_ingreso": ruta[2],
	"Fecha_compromiso": ruta[3],
	"Region": ruta[4],
	"Comuna": ruta[5],
	"Descripcion": ruta[6],
	"Bultos": ruta[7],
	"Estado": ruta[8],
	"Subestado": ruta[9],
	"Verificado": ruta[10],
	"Recibido": ruta[11]
	}

def rutas_de_pendientes_schema(rutas):
    return [ruta_de_pendientes(ruta) for ruta in rutas]



import re

def reemplazar_caracteres(texto,direccion_original):
    # Expresión regular para buscar caracteres no imprimibles
    patron = re.compile(r'[\x00-\x1F\x7F-\x9F]')
    
    # Buscar coincidencias en el texto
    coincidencias = patron.findall(texto)
    
    # Si hay coincidencias, reemplazarlas
    if coincidencias:
        for c in coincidencias:
            texto = direccion_original  # Reemplazar por el texto deseado
    
    return texto


def ruta_de_pendientes_con_ruta(ruta):
     
	texto = reemplazar_caracteres(ruta[13], ruta[15])
    
	return {
	"Origen": ruta[0],
	"Cod_entrega": ruta[1],
	"Fecha_ingreso": ruta[2],
	"Fecha_compromiso": ruta[3],
	"Region": ruta[4],
	"Comuna": ruta[5],
	"Descripcion": ruta[6],
	"Bultos": ruta[7],
	"Estado": ruta[8],
	"Subestado": ruta[9],
	"Verificado": ruta[10],
	"Recibido": ruta[11],
    "Nombre_ruta" : ruta[12],
    "Direccion": texto,
    "Talla" : ruta[14]
	}

def rutas_de_pendientes_con_ruta_schema(rutas):
    return [ruta_de_pendientes_con_ruta(ruta) for ruta in rutas]