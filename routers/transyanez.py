from fastapi import APIRouter,status,UploadFile, File,HTTPException
from database.client import transyanezConnection , reportesConnection
from fastapi.responses import FileResponse
from openpyxl import Workbook
from datetime import datetime
from database.models.colaboradores.bitacora import BitacoraTransporte
from database.models.colaboradores.colaborador import Colaboradores,DetallesPago
from database.models.colaboradores.vehiculos import Vehiculos
from database.schema.transporte.colaborador import colaboradores_schema, detalle_pagos_schema
from database.schema.transporte.vehiculo import vehiculos_schema
from database.schema.transporte.estado import estados_transporte_schema
from lib.validar_rut import valida_rut
from lib.password import hash_password
import psycopg2.errors
import os

router = APIRouter(tags=["transporte"],prefix="/api/transporte")


connTY = transyanezConnection()

conn = reportesConnection()

dia_actual = datetime.today().strftime('%Y-%m-%d')

nombre_archivo = f"resumen_vehiculos_portal_{dia_actual}".format(dia_actual)

# Crear un nuevo libro de trabajo y hoja de cálculo

@router.get("/resumen_vehiculos_portal")
async def download_resumen_vehiculos_portal():

    results = connTY.get_vehiculos_portal()
    wb = Workbook()
    ws = wb.active
    results.insert(0, ("Compañia","Región Origen","Patente","Estado","Tipo","Caracteristicas","Marca","Modelo","Año","Región","Comuna"))

    for row in results:
        ws.append(row)
    
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter # get column letter
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column].width = adjusted_width
    
    wb.save("resumen_vehiculos_portal_yyyymmdd.xlsx")

    return FileResponse("resumen_vehiculos_portal_yyyymmdd.xlsx")



@router.post("/agregar/colaborador")
async def agregar_nuevo_colaborador(body : Colaboradores):
    try:
        body.Email_representante_legal = body.Email
        data = body.dict()
        conn.insert_colaborador(data)

        id_razon_social = conn.buscar_id_colab_por_rut(body.Rut)

        print(body)
        return {
            "message": "Colaborador agregado correctamente",
            "razon" : id_razon_social[0]

        }
    except psycopg2.errors.UniqueViolation as error:
        # Manejar la excepción UniqueViolation específica
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: El rut {body.Rut} ya se encuentra registrado")

    except Exception as error:
        print(error)
        # Manejar otras excepciones
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error al agregar el detalle de pago.")



@router.put("/actualizar/colaborador")
async def actualizar_datos_colaborador(body : Colaboradores):
    try:
        body.Email_representante_legal = body.Email
        data = body.dict()
        conn.update_datos_colaborador(data)

        id_razon_social = conn.buscar_id_colab_por_rut(body.Rut)

        print(body)
        return {
            "message": "Colaborador actualizado correctamente",
            "razon" : id_razon_social[0]

        }
    except psycopg2.errors.UniqueViolation as error:
        # Manejar la excepción UniqueViolation específica
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: El rut {body.Rut} ya se encuentra registrado")

    except Exception as error:
        print(error)
        # Manejar otras excepciones
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error al agregar el detalle de pago.")
    
@router.get("/activar/colaborador")
async def activar_colaborador(rut : str,activar: bool):

    conn.activar_colab(rut,activar)
    return {
        "message": "Colaborador activado correctamente",
    }


@router.post("/agregar/colaborador/datos/banco")
async def agregar_detalle_banco(body : DetallesPago):
    data = body.dict()
    conn.insert_detalle_pagos(data)

    return {
        "message": "Colaborador agregado correctamente",
    }


@router.put("/actualizar/colaborador/datos/banco")
async def agregar_detalle_banco(body : DetallesPago):
    data = body.dict()
    conn.update_datos_detalle_pago(data)

    return {
        "message": "Colaborador actualizado correctamente",
    }


@router.post("/agregar/vehiculos")
async def agregar_detalle_banco(body : Vehiculos ):
    # razon_id = conn.buscar_id_colab_por_rut(body.Rut_colaborador)[0]

    body.Razon_id = 1
    body.Estado = False

    data = body.dict()

    conn.insert_vehiculo_transporte(data)

    return {
        "message": "vehiculo agregado correctamente",
    }


@router.put("/actualizar/datos/vehiculo")
async def actualizar_datos_vehiculo(body : Vehiculos):
    body.Razon_id= conn.buscar_id_colab_por_rut(body.Rut_colaborador)[0]
    data = body.dict()
    conn.update_datos_vehiculo(data)

    return {
        "message": "Vehiculo actualizado correctamente",
    }

@router.get("/buscar/vehiculos")
async def get_lista_vehiculos():
    
    result = conn.buscar_vehiculos()

    return vehiculos_schema(result)


@router.get("/ver/colaboradores")
async def get_lista_colaboradores():
    
    result = conn.buscar_colaboradores()

    return colaboradores_schema(result)

@router.get("/buscar/colaboradores")
async def get_lista_colaboradores(nombre : str):
    
    result = conn.buscar_colaboradores_por_nombre(nombre)

    return colaboradores_schema(result)

@router.get("/verificar/razon_social")
async def verificar_razon_social(rut : str):
    
    result = conn.buscar_id_colab_por_rut(rut)

    if result:
        return {
            'message': "el rut del colaborador existe",
            'razon_id': result[0],
            'razon_social' : result[1]
        }
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"El rut no se encuentra registrado como colaborador")


@router.get("/ver/detalles_pago")
async def get_lista_colaboradores(id : str):
    
    result = conn.buscar_detalle_pago(id)

    return detalle_pagos_schema(result)


@router.get("/buscar/vehiculo/{filtro}")
async def get_lista_colaboradores(filtro : str):
    
    result = conn.buscar_vehiculos_por_filtro(filtro)

    return vehiculos_schema(result)


@router.post("/colaboradores/subir-archivo", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo(tipo_archivo : str, nombre : str, file: UploadFile = File(...)):

    directorio  = os.path.abspath(f"pdfs/transporte/colaboradores/{tipo_archivo}")
    print(directorio)
    nombre_hash = hash_password(tipo_archivo+nombre)

    nuevo_nombre = nombre_hash +'.pdf'

    ruta = os.path.join(directorio,nuevo_nombre)

    with open(ruta, "wb") as f:
        contents = await file.read()
        # print("pase por aqui")
        f.write(contents)

    if tipo_archivo == 'cert_rrpp':
        conn.agregar_pdf_colab_rrpp(f'pdfs/transporte/colaboradores/{tipo_archivo}/{nuevo_nombre}',nombre)

    if tipo_archivo == 'cert_vig_poderes':
        conn.agregar_pdf_colab_poderes(f'pdfs/transporte/colaboradores/{tipo_archivo}/{nuevo_nombre}',nombre)

    if tipo_archivo == 'constitucion_legal':
        conn.agregar_pdf_colab_constitucion(f'pdfs/transporte/colaboradores/{tipo_archivo}/{nuevo_nombre}',nombre)

    if tipo_archivo == 'registro_comercio':
        conn.agregar_pdf_colab_registro_comercio(f'pdfs/transporte/colaboradores/{tipo_archivo}/{nuevo_nombre}',nombre)

    if tipo_archivo == 'documento_bancario':
        conn.agregar_pdf_detalle_venta(f'pdfs/transporte/colaboradores/{tipo_archivo}/{nuevo_nombre}',nombre)

    return {
        "message" : "ok"
    }

@router.get("/descargar")
def download_file(name_file: str):
    nombre_file = name_file.split('/')[4]

    print(name_file)
    print(nombre_file)
    
    return FileResponse(name_file, filename=nombre_file)

@router.post("/vehiculos/subir-archivo", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo(tipo_archivo : str, nombre : str, file: UploadFile = File(...)):

    directorio  = os.path.abspath(f"pdfs/transporte/vehiculos/{tipo_archivo}")
    print(directorio)
    nombre_hash = hash_password(tipo_archivo+nombre)

    nuevo_nombre = nombre_hash +'.pdf'

    ruta = os.path.join(directorio,nuevo_nombre)

    with open(ruta, "wb") as f:
        contents = await file.read()
        # print("pase por aqui")
        f.write(contents)

    if tipo_archivo == 'cert_gases':
        conn.agregar_pdf_vehiculo_cert_gases(f'pdfs/transporte/vehiculos/{tipo_archivo}/{nuevo_nombre}',nombre)

    if tipo_archivo == 'padron':
        conn.agregar_pdf_padron(f'pdfs/transporte/vehiculos/{tipo_archivo}/{nuevo_nombre}',nombre)

    if tipo_archivo == 'permiso_circulacion':
        conn.agregar_pdf_permiso_circulacion(f'pdfs/transporte/vehiculos/{tipo_archivo}/{nuevo_nombre}',nombre)

    if tipo_archivo == 'revision_tecnica':
        conn.agregar_pdf_revision_tecnica(f'pdfs/transporte/vehiculos/{tipo_archivo}/{nuevo_nombre}',nombre)

    if tipo_archivo == 'soap':
        conn.agregar_pdf_soap(f'pdfs/transporte/vehiculos/{tipo_archivo}/{nuevo_nombre}',nombre)


    return {
        "message" : "ok"
    }

@router.post("/registrar/bitacora")
async def registrar_bitacora_transporte(body : BitacoraTransporte ):
    data = body.dict()
    conn.insert_vehiculo_transporte(data)

    return {
        "message": "registro realizado correctamente",
    }

@router.get("/estados")
async def get_estados_transporte():
    result = conn.obtener_estados_transporte()
    return estados_transporte_schema(result)

@router.post("/usuario/subir-imagen", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo(tipo_archivo : str, nombre : str, file: UploadFile = File(...)):

    directorio  = os.path.abspath(f"pdfs/transporte/vehiculos/{tipo_archivo}")
    print(directorio)
    # nombre_hash = hash_password(tipo_archivo+nombre)

    nuevo_nombre = nombre

    ruta = os.path.join(directorio,nuevo_nombre)

    with open(ruta, "wb") as f:
        contents = await file.read()
        # print("pase por aqui")
        f.write(contents)


    conn.agregar_pdf_vehiculo_cert_gases(f'pdfs/transporte/vehiculos/{tipo_archivo}/{nuevo_nombre}',nombre)


    return {
        "message" : "ok"
    }


