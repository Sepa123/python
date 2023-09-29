def generar_codigo_ty_nota_venta_rsv(numero):
    # Convierte el número en una cadena y rellena con ceros a la izquierda
    numero_formateado = str(numero).zfill(7)
    # Concatena el prefijo con el número formateado
    codigo = "tyrsv" + numero_formateado
    return codigo

# Ejemplo de uso
numero = int(input("Ingrese un número: "))  # Pide al usuario que ingrese un número
codigo_generado = generar_codigo_ty_nota_venta_rsv(numero)
print("El código generado es:", codigo_generado)


# from main import auth_user


# auth_user("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJwb2tlbW9uIiwiZXhwIjoxNjgzMjEzMzUzfQ.DBiYv2ugeubz4dfFr7mWqM3LEYmMg_rRVMWvnAON_Hc")