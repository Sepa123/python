from datetime import datetime

def formatear_fecha(fecha_str):
    try:
        fecha = datetime.strptime(fecha_str, "%Y-%m-%d")
        return fecha.strftime("%d-%m-%Y")
    except (ValueError, TypeError):
        return None

def ruta_de_pendientes(ruta):
	return {
	"Origen": ruta[0],
    "Id_cliente": ruta[1],
	"Cod_entrega": ruta[2],
	"Fecha_ingreso": formatear_fecha(ruta[3]),
	"Fecha_compromiso": formatear_fecha(ruta[4]),
	"Region": ruta[5],
	"Comuna": ruta[6],
	"Descripcion": ruta[7],
	"Bultos": ruta[8],
	"Estado": ruta[9],
	"Subestado": ruta[10],
	"Verificado": ruta[11],
	"Recibido": ruta[12],
    "Observacion": ruta[13],
    "Alerta": ruta[14]
	}

def rutas_de_pendientes_schema(rutas):
    return [ruta_de_pendientes(ruta) for ruta in rutas]




import re

def reemplazar_caracteres(texto,direccion_original):
    # Expresión regular para buscar caracteres no imprimibles
    patron = re.compile(r'[\x00-\x1F\x7F-\x9F]')
    

    if(texto is None):
          return direccion_original
    

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