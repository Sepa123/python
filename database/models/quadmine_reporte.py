from pydantic import BaseModel
import re

def quitar_caracteres(texto : str):
    texto_limpio = re.sub(r'[^\x00-\x7F]+', '', texto)
    return texto_limpio


class ReportesQuadmine(BaseModel):
    codigo_cliente: str
    nombre: str
    calle: str
    ciudad: str
    estado: str
    latitud: str
    longitud: str
    telefono: str
    email: str
    fecha_pedido: str
    operacion_e_r: str
    codigo_producto: str
    descripcion_producto: str
    cantidad_producto: int
    peso: int
    volumen: int
    dinero: int
    duracion_min: int
    ventana_horario_1: str
    ventana_horario_2: str
    notas: str
    agrupador: str
    email_remitente: str
    email_pedido: str
    vehiculo: str
    habilidades: str

    # def __init__(self, codigo_cliente: str, nombre: str, calle: str, ciudad: str, estado: str, latitud: str, longitud: str, telefono: str, email: str, fecha_pedido: str, operacion_e_r: str, codigo_producto: str, descripcion_producto: str, cantidad_producto: int, peso: int, volumen: int, dinero: int, duracion_min: int, ventana_horario_1: str, ventana_horario_2: str, notas: str, agrupador: str, email_remitente: str, email_pedido: str, vehiculo: str, habilidades: str) -> None:
    #     self.codigo_cliente = codigo_cliente
    #     self.nombre = nombre
    #     self.calle = calle
    #     self.ciudad = ciudad
    #     self.estado = estado
    #     self.latitud = latitud
    #     self.longitud = longitud
    #     self.telefono = telefono
    #     self.email = email
    #     self.fecha_pedido = fecha_pedido
    #     self.operacion_e_r = operacion_e_r
    #     self.codigo_producto = codigo_producto
    #     self.descripcion_producto = descripcion_producto
    #     self.cantidad_producto = cantidad_producto
    #     self.peso = peso
    #     self.volumen = volumen
    #     self.dinero = dinero
    #     self.duracion_min = duracion_min
    #     self.ventana_horario_1 = ventana_horario_1
    #     self.ventana_horario_2 = ventana_horario_2
    #     self.notas = notas
    #     self.agrupador = agrupador
    #     self.email_remitente = email_remitente
    #     self.email_pedido = email_pedido
    #     self.vehiculo = vehiculo
    #     self.habilidades = habilidades
        
# def quadmind_reporte_schema(db_data):

     