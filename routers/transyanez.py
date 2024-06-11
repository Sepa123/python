from typing import Dict, List
from fastapi import APIRouter,status,UploadFile, File,HTTPException
from database.client import transyanezConnection , reportesConnection
from fastapi.responses import FileResponse
from openpyxl import Workbook
from datetime import datetime
from database.models.colaboradores.bitacora import BitacoraTransporte
from database.models.colaboradores.colaborador import Colaboradores,DetallesPago,DesvincularColaborador
from database.models.colaboradores.vehiculos import Vehiculos,AsignarOperacion, VehiculosExcel,cambiarEstadoVehiculo
from database.models.colaboradores.persona import Usuario
from database.models.operaciones.peso_volumetrico import PesoVolumetrico
from database.schema.transporte.colaborador import colaboradores_schema, detalle_pagos_schema
from database.schema.transporte.vehiculo import vehiculos_schema ,operacion_vehiculo_schema, vehiculos_y_op_schema
from database.schema.transporte.usuario import usuarios_transporte_schema
from database.schema.transporte.estado import estados_transporte_schema
from lib.validar_rut import valida_rut
from lib.password import hash_password
import psycopg2.errors
import os
import lib.excel_generico as excel


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
async def actualizar_detalle_banco(body : DetallesPago):
    data = body.dict()
    conn.update_datos_detalle_pago(data)

    return {
        "message": "Colaborador actualizado correctamente",
    }


@router.post("/agregar/vehiculos")
async def agregar_datos_vehiculos(body : Vehiculos ):
    razon_id = conn.buscar_id_colab_por_rut(body.Rut_colaborador)[0]
    
    body.Razon_id = razon_id
    print(body)
    if body.Gps == True:
        data_gps= body.dict()
        conn.agregar_datos_gps(data_gps)
        id_gps = conn.get_max_id_gps()[0]
        print(id_gps)

        body.Id_gps = id_gps
    else:
        body.Id_gps = None

    data = body.dict()

    conn.insert_vehiculo_transporte(data)

    

    return {
        "message": "vehiculo agregado correctamente",
    }


@router.put("/actualizar/datos/vehiculo")
async def actualizar_datos_vehiculo(body : Vehiculos):
    body.Razon_id= conn.buscar_id_colab_por_rut(body.Rut_colaborador)[0]
    ### si se clickeo el gps
    if body.Gps == True:
        if body.Id_gps == None or body.Id_gps == 'null':
            data_gps= body.dict()
            conn.agregar_datos_gps(data_gps)
            id_gps = conn.get_max_id_gps()[0]

            body.Id_gps = id_gps
        else:
            data_gps= body.dict()
            conn.actualizar_datos_gps(data_gps)
    ### si no se clickeo el gps
    elif body.Gps == False:
        if body.Id_gps != None:
            data_gps= body.dict()
            conn.actualizar_datos_gps_si_se_desactiva_gps(data_gps)
        else:
            body.Id_gps = None
    else:
        body.Id_gps = None

    data = body.dict()
    conn.update_datos_vehiculo(data)

    return {
        "message": "Vehiculo actualizado correctamente",
    }


@router.put("/actualizar/estado/vehiculo")
async def actualizar_datos_vehiculo(body : cambiarEstadoVehiculo):
    # data = body.dict()
    
    conn.cambiar_estado_a_vehiculo(body.id)
    

    return {
        "message": "Vehiculo actualizado correctamente",
    }


@router.get("/revisar/operacion/vehiculo")
async def get_lista_vehiculos(vehiculo : int):
    
    result = conn.revisar_operacion_asignada_a_vehiculo(vehiculo)

    return operacion_vehiculo_schema(result)

@router.put("/asignar/operacion/vehiculo")
async def actualizar_datos_vehiculo(body : AsignarOperacion):
    data = body.dict()
    conn.asignar_operacion_a_vehiculo(data)
    

    return {
        "message": "Operación asignada correctamente",
    }

@router.get("/buscar/vehiculos")
async def get_lista_vehiculos():
    
    result = conn.buscar_vehiculos()

    return vehiculos_schema(result)


@router.get("/buscar/vehiculos/operacion")
async def get_lista_vehiculos_mas_operacion():
    
    result = conn.buscar_vehiculos_y_operacion_pta()

    return vehiculos_y_op_schema(result)


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

    if tipo_archivo == 'pdf_contrato':
        conn.agregar_pdf_contrato_colaborador(f'pdfs/transporte/colaboradores/{tipo_archivo}/{nuevo_nombre}',nombre)

    return {
        "message" : "ok"
    }

@router.get("/descargar")
def download_file(name_file: str):
    # nombre_file = name_file.split('/')[4]

    print(name_file)
    # print(nombre_file)
    
    return FileResponse(name_file)

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

@router.post("/usuario/subir-archivo", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo_usuario(tipo_archivo : str, nombre : str, file: UploadFile = File(...)):

    directorio  = os.path.abspath(f"pdfs/transporte/usuarios/{tipo_archivo}")
    print(directorio)
    # nombre_hash = hash_password(tipo_archivo+nombre)

    nuevo_nombre = nombre

    ruta = os.path.join(directorio,nuevo_nombre)

    with open(ruta, "wb") as f:
        contents = await file.read()
        # print("pase por aqui")
        f.write(contents)


    if tipo_archivo == 'cedula_identidad':
        conn.agregar_pdf_cedula_identidad(f'pdfs/transporte/usuarios/{tipo_archivo}/{nuevo_nombre}',nombre)

    if tipo_archivo == 'cert_antecedentes':
        conn.agregar_pdf_antecedentes(f'pdfs/transporte/usuarios/{tipo_archivo}/{nuevo_nombre}',nombre)

    if tipo_archivo == 'contrato':
        conn.agregar_pdf_contrato(f'pdfs/transporte/usuarios/{tipo_archivo}/{nuevo_nombre}',nombre)

    if tipo_archivo == 'licencia_conducir':
        conn.agregar_pdf_licencia_conducir(f'pdfs/transporte/usuarios/{tipo_archivo}/{nuevo_nombre}',nombre)


    return {
        "message" : "ok"
    }


@router.post("/agregar/usuario")
async def agregar_tripulacion_usuario(body : Usuario ):
    try:
        razon_id = conn.buscar_id_colab_por_rut(body.Rut_razon_social)[0]

        body.Id_razon_social=razon_id
        data = body.dict()

        conn.agregar_usuario_transporte(data)

        return {
            "message": "Usuario agregado correctamente",
        }
    except psycopg2.errors.UniqueViolation as error:
        # Manejar la excepción UniqueViolation específica
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: El rut {body.Rut} ya se encuentra registrado")

    except Exception as error:
        print(error)
        # Manejar otras excepciones
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error al agregar el detalle de pago.")
    


@router.put("/actualizar/datos/usuario")
async def actualizar_datos_usuario(body : Usuario):
    # body.Razon_id= conn.buscar_id_colab_por_rut(body.Rut_colaborador)[0]
    data = body.dict()
    if body.Fec_venc_lic_conducir == 'null':
        body.Fec_venc_lic_conducir == None
    conn.update_datos_usuario(data)

    return {
        "message": "Usuario actualizado correctamente",
    }

@router.get("/usuarios")
async def get_usuarios_transporte():
    result = conn.lista_usuario_transporte()
    return usuarios_transporte_schema(result)


@router.get("/tripulacion/tipo")
async def get_tipo_tripulacion():
    return [
                {
                    "Id" : 1,
                    "Tripulacion" : "Chofer"
                },
                {
                    "Id" : 2,
                    "Tripulacion" : "Peoneta"
                }
            ]


@router.post("/subir/foto-perfil", status_code=status.HTTP_202_ACCEPTED)
async def subir_archivo(nombre : str, file: UploadFile = File(...)):

    directorio  = os.path.abspath(f"image/foto_perfil")
    # print(directorio)
    # nombre_hash = hash_password(tipo_archivo+nombre)

    ruta = os.path.join(directorio,file.filename)

    with open(ruta, "wb") as f:
        contents = await file.read()
        # print("pase por aqui")
        f.write(contents)


    conn.agregar_jpg_foto_perfil(f'image/foto_perfil/{file.filename}',nombre)


    return {
        "message" : "ok"
    }


# buscar_colab_registrados
@router.get("/verificar/colab/vehiculo")
async def verificar_colab_registrado_vehiculos(rut : str):
    result = conn.verificar_colab_registrado_vehiculos(rut)
    if len(result) == 0:
        return {
            "message": 'el colaborador no se encuentra registrado en vehiculos'
        }
    else:
        return {
            "message": 'el colaborador ya se encuentra registrado en vehiculos'
        }

# buscar_colab_registrados
@router.get("/verificar/colab/tripulacion")
async def verificar_colab_registrado_vehiculos(rut : str):
    result = conn.verificar_colab_registrado_vehiculos(rut)
    if len(result) == 0:
        return {
            "message": 'el colaborador no se encuentra registrado en tripulacion'
        }
    else:
        return {
            "message": 'el colaborador ya se encuentra registrado en tripulacion'
        }
    
@router.put("/actualizar/estado/tripulacion")
async def actualizar_estado_a_usuario_tripulacion(body : cambiarEstadoVehiculo):
    # data = body.dict()
    
    conn.cambiar_estado_a_usuario_tripulacion(body.id)
    

    return {
        "message": "Vehiculo actualizado correctamente",
    }


@router.delete("/eliminar/operacion/vehiculo") 
async def eliminar_operacion_vehiculo(id : str):
     try:
          results = conn.eliminar_operacion_vehiculo_asignado(id)
          if (results == 0): print("La operaciona asignada no existe")
          return { "message" : "Operacion eliminada correctamente"}
     except:
          print("error en /eliminar/operacion/vehiculo")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")
       
@router.post("/desvincular/colaborador")
async def desvincular_colaborador(body : DesvincularColaborador ):
    try:
        # razon_id = conn.buscar_id_colab_por_rut(body.Rut_razon_social)[0]

        # body.Id_razon_social=razon_id
        data = body.dict()

        conn.update_desactivar_colaborador(data)

        return {
            "message": "Colaborador desvinculado correctamente",
        }
    except psycopg2.errors.UniqueViolation as error:
        # Manejar la excepción UniqueViolation específica
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: El rut {body.Rut} ya se encuentra registrado")

    except Exception as error:
        print(error)
        # Manejar otras excepciones
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error al agregar el detalle de pago.")
    
    # buscador de vehiculos por operacion y centro peracion
@router.get("/buscar/vehiculos/filtro")
async def buscar_vehiculos_por_operacion(id_op : int, id_co : int):
    result = conn.buscar_vehiculos_ppu_operacion(id_op,id_co)

    datos = [vehiculo[0] for vehiculo in result]

    return {
        'Vehiculo' : datos
    } 

@router.post("/vehiculos/descargar")
async def descargar_vehiculos_filtro(pendientes : List[VehiculosExcel]):

    print(pendientes)

    tupla = excel.objetos_a_tuplas(pendientes)

    nombre_filas = ( 'Patente', 'Razón Social', "Tipo Vehículo", "Región Disponible", 
                     "GPS", "Disponible","Habilitado","Fecha de registro")
    nombre_excel = f"Vehiculos_filtrados"

    return excel.generar_excel_generico(tupla,nombre_filas,nombre_excel)



#### Peso volumetrico Rodrigo

@router.post("/skuPesoVolumetrico")
async def skuPesoVolumetrico(body: PesoVolumetrico):
    try:
        conn.insert_peso_volumetrico_sku(body)
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/buscar/sku_descripcion")
async def Obtener_datos(sku_descripcion: str):
     # Consulta SQL para obtener datos (por ejemplo)
    # consulta = f"select * from operacion.busca_sku_entrada('{sku_descripcion}');"
    # Ejecutar la consulta utilizando nuestra función
    datos = conn.buscar_entrada_sku(sku_descripcion)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "sku" : fila[0],
                                "descripcion": fila[1],
                                "bultos" : fila[2],
                                "alto": fila [3],
                                "ancho": fila[4],
                                "profundidad" : fila[5],
                                "peso_kg" : fila[6],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.get("/mostrarDatosTable")
async def Obtener_datos(sku: str):
    datos = conn.buscar_sku_o_descripcion(sku)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{  
                                "sku": fila[0],
                                "descripcion": fila[1],
                                "alto": fila [2],
                                "ancho": fila[3],
                                "profundidad" : fila[4],
                                "peso_kg" : fila[5],
                                "bultos" : fila[6],
                                "pv": fila[7],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")