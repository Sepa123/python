from datetime import datetime, date

def formatear_fecha(fecha_input):
    
    if isinstance(fecha_input, date):
        return fecha_input.strftime("%d-%m-%Y")
    elif isinstance(fecha_input, str):
        try:
            fecha = datetime.strptime(fecha_input, "%Y-%m-%d")
            return fecha.strftime("%d-%m-%Y")
        except ValueError:
            return None
    else:
        return None
    
def ruta_de_pendientes(ruta):
	return {
	"Origen": ruta[0],
    "Id_cliente": ruta[1],
	"Cod_entrega": ruta[2],
	"Fecha_ingreso": formatear_fecha(ruta[3]),
	"Fecha_compromiso":  formatear_fecha(ruta[4]),
    "Fecha_reprogramado":  formatear_fecha(ruta[5]),
	"Region": ruta[6],
	"Comuna": ruta[7],
	"Descripcion": ruta[8],
	"Bultos": ruta[9],
	"Estado": ruta[10],
	"Subestado": ruta[11],
	"Verificado": ruta[12],
	"Recibido": ruta[13],
    "Observacion": ruta[14],
    "Alerta": ruta[15]
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