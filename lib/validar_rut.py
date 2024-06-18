import re

def detectar_rut(texto):
    # Expresión regular para detectar el RUT en el formato mencionado
    patron_rut = re.compile(r'\b\d{1,3}\.\d{3}\.\d{3}-[\dkK]\b')
    
    # Buscar todas las coincidencias en el texto
    coincidencias = patron_rut.findall(texto)
    
    return coincidencias

def convertir_rut(formato_antiguo):
    # Usamos re.sub para reemplazar los puntos por una cadena vacía
    formato_nuevo = re.sub(r'\.', '', formato_antiguo)
    return formato_nuevo

def valida_rut(rut_completo):

    rut_completo = rut_completo.replace("‐","-");
    if not re.match(r'^[0-9]+[-|‐]{1}[0-9kK]{1}$', rut_completo, re.IGNORECASE):
        return False
    # rut, digv = rut_completo.split('-')
    rut, digv = rut_completo.split('-')

    print(rut)
    print(digv)

    # rut = rut_completo[:-2]
    # digv = rut_completo[-1]

    if digv == 'K':
        digv = 'k'
    return calcular_dv(rut) == digv

def calcular_dv(T):
    M = 0
    S = 1
    while T:
        S = (S + int(T) % 10 * (9 - M % 6)) % 11
        M += 1
        T = int(T) // 10
    return str(S - 1) if S else 'k'

