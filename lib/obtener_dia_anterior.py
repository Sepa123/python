from datetime import datetime, timedelta

def obtener_dia_anterior():
    hoy = datetime.now().date()
    if hoy.isoweekday() == 1:  # Si hoy es domingo (1 en ISO)
        fecha_anterior = hoy - timedelta(days=2)
    else:
        fecha_anterior = hoy - timedelta(days=1)
    return fecha_anterior