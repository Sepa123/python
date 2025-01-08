from typing import Dict, List
from fastapi import APIRouter,status,UploadFile, File,HTTPException
from database.client import transyanezConnection , reportesConnection
from fastapi.responses import FileResponse
from openpyxl import Workbook
from datetime import datetime
from database.models.colaboradores.bitacora import BitacoraTransporte
from database.models.colaboradores.colaborador import Colaboradores,DetallesPago,DesvincularColaborador
from database.models.colaboradores.vehiculos import Vehiculos,AsignarOperacion, VehiculosExcel,cambiarEstadoVehiculo,VehiculosExcelResumen
from database.models.colaboradores.persona import Usuario
from database.models.operaciones.peso_volumetrico import PesoVolumetrico
from database.models.colaboradores.reclutamiento import ComentarioRecluta, Reclutamiento
from database.schema.transporte.colaborador import colaboradores_schema, detalle_pagos_schema,motivo_desvinculacion_schema
from database.schema.transporte.vehiculo import vehiculos_schema ,operacion_vehiculo_schema, vehiculos_y_op_schema
from database.schema.transporte.usuario import usuarios_transporte_schema
from database.schema.transporte.estado import estados_transporte_schema
from lib.validar_rut import convertir_rut, detectar_rut, valida_rut
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

        if(body.Tipo_razon == 7):
            body.Tipo_razon = 11
        else:
            body.Tipo_razon = 7

        data = body.dict()
        conn.insert_colaborador(data)

        id_razon_social = conn.buscar_id_colab_por_rut(body.Rut)

        conn.insert_bitacora_transporte(data)

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

        conn.insert_bitacora_transporte(data)

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
    
@router.post("/activar/colaborador")
async def activar_colaborador(body : DesvincularColaborador):

    conn.activar_colab(body.Rut,True)


    body.Modificacion = f'Se ha activado al colaborador con rut {body.Rut}'

    data = body.dict()

    conn.insert_bitacora_transporte(data)
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
    try:
        razon_id = conn.buscar_id_colab_por_rut(body.Rut_colaborador)[0]
        body.Razon_id = razon_id
        
        if body.Gps == True:
            data_gps= body.dict()
            conn.agregar_datos_gps(data_gps)
            id_gps = conn.get_max_id_gps()[0]
            body.Id_gps = id_gps
        else:
            body.Id_gps = None

        data = body.dict()

        conn.insert_vehiculo_transporte(data)

        conn.insert_bitacora_transporte(data)

        return {
        "message": "vehiculo agregado correctamente",
         }   

    except psycopg2.errors.UniqueViolation as error:

        unique = error.pgerror.split('"')[1].split('"')[0]

        if unique == 'transporte_vehiculo_rut_unique':
            conn.eliminar_gps(body.Imei)
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: La patente {body.Ppu} ya se encuentra registrado")
        
        if unique == 'transporte_gps_imei_unique' :
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: El IMEI {body.Imei} ya se encuentra registrado")
        
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: se encuentran datos duplicados")
        
    except Exception as error:
        print(error)
        # Manejar otras excepciones
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error al agregar la patente.")


@router.put("/actualizar/datos/vehiculo")
async def actualizar_datos_vehiculo(body : Vehiculos):
    if body.Ano == 'null' : body.Ano = None
    if body.Marca == 'null' : body.Marca = None
    if body.Capacidad_carga_kg == 'null' : body.Capacidad_carga_kg = None
    if body.Capacidad_carga_m3 == 'null' : body.Capacidad_carga_m3 = None
    if body.Platform_load_capacity_kg == 'null' : body.Platform_load_capacity_kg = None
    if body.Crane_load_capacity_kg == 'null' : body.Crane_load_capacity_kg = None
    if body.Id_gps == 'null' : body.Id_gps = None

    body.Razon_id= conn.buscar_id_colab_por_rut(body.Rut_colaborador)[0]

    try:

        ### si el imei es null,ignorar el agregar gps

        if body.Imei == 'null' or body.Imei == None:
            pass
        else:
            ### si se clickeo el gps
            if body.Gps == True:
                if body.Id_gps == None or body.Id_gps == 'null':
                    data_gps= body.dict()
                    conn.agregar_datos_gps(data_gps)
                    id_gps = conn.get_max_id_gps()[0]
                    # print('pase por aca')
                    body.Id_gps = id_gps
                else:
                    # print('qhe es eesos')
                    data_gps= body.dict()
                    conn.actualizar_datos_gps(data_gps)
            ### si no se clickeo el gps
            elif body.Gps == False:
                if body.Id_gps != None or body.Id_gps != 'null':
                    data_gps= body.dict()
                    conn.actualizar_datos_gps_si_se_desactiva_gps(data_gps)
                    # print('paseee por acasss')
                else:
                    # body.Id_gps = None
                    # print('pase por acasss')
                    data_gps= body.dict()
                    conn.agregar_datos_gps(data_gps)
                    id_gps = conn.get_max_id_gps()[0]
                    body.Id_gps = id_gps
            else:
                # body.Id_gps = None
                # print('asddadadada')
                data_gps= body.dict()
                conn.agregar_datos_gps(data_gps)
                id_gps = conn.get_max_id_gps()[0]
                body.Id_gps = id_gps
        # print('ptm')
        data = body.dict()
        conn.update_datos_vehiculo(data)

        conn.insert_bitacora_transporte(data)

        return {
            "message": "Vehiculo actualizado correctamente",
        }
    except psycopg2.errors.UniqueViolation as error:

        unique = error.pgerror.split('"')[1].split('"')[0]

        if unique == 'transporte_vehiculo_rut_unique':
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: La patente {body.Ppu} ya se encuentra registrado")
        
        if unique == 'transporte_gps_imei_unique' :
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: El IMEI {body.Imei} ya se encuentra registrado")
        
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: se encuentran datos duplicados")
    
    except psycopg2.errors.NotNullViolation as error:

        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: Se debe agregar un IMEI ")
        
    except Exception as error:
        print(error)
        # Manejar otras excepciones
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error al agregar la patente.")


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
    is_rut = len(detectar_rut(nombre))

    if is_rut != 0:
        nombre = convertir_rut(nombre)
    
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
    nombre_hash = f'{nombre}_'+file.filename

    nuevo_nombre = nombre_hash 

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
    nombre_hash = f'{nombre}_'+file.filename

    nuevo_nombre = nombre_hash 

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
    conn.insert_bitacora_transporte(data)

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

    nombre_hash = f'{nombre}_'+file.filename

    nuevo_nombre = nombre_hash 

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

        conn.insert_bitacora_transporte(data)

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
    
    if body.Fec_venc_lic_conducir == 'null':
        body.Fec_venc_lic_conducir = None

    data = body.dict()
    conn.update_datos_usuario(data)

    conn.insert_bitacora_transporte(data)

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
     

@router.get("/colaborador/desvinculacion")
async def motivos_desvinculacion():
    result = conn.motivo_desvinculacion_colaborador()
    return motivo_desvinculacion_schema(result)
       
@router.post("/desvincular/colaborador")
async def desvincular_colaborador(body : DesvincularColaborador ):
    try:
        # razon_id = conn.buscar_id_colab_por_rut(body.Rut_razon_social)[0]
        

        body.Modificacion = f'Se ha desvinculado al colaborador con rut {body.Rut}'
        # body.Id_razon_social=razon_id
        data = body.dict()

        conn.update_desactivar_colaborador(data)


        conn.insert_bitacora_transporte(data)

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

    if id_op == 0:
        result = conn.buscar_vehiculos_ppu_operacion(id_op,id_co)
    elif id_co == 0:
        result = conn.buscar_vehiculos_ppu_operacion(id_op,id_co)
    else:
        result = conn.buscar_vehiculos_ppu_operacion_co(id_op,id_co)
   
    

    datos = [vehiculo[0] for vehiculo in result]

    return {
        'Vehiculo' : datos
    } 

@router.post("/vehiculos/descargar")
async def descargar_vehiculos_filtro(pendientes : List[VehiculosExcel]):

    print(pendientes)


    tupla = excel.objetos_a_tuplas(pendientes)

    nombre_filas = ( 'Patente', 'Razón Social', "Tipo Vehículo", 'Operación','Centro operación', "Región Disponible", 
                     "GPS", "Disponible","Habilitado","Fecha de registro")
    nombre_excel = f"Vehiculos_filtrados"

    return excel.generar_excel_generico(tupla,nombre_filas,nombre_excel)


@router.get("/marcas/vehiculos")
async def buscar_marcas_vehiculos():
    datos = conn.obtener_marcas_vehiculos()
    return datos[0]



@router.post("/vehiculos/descargar/resumen")
async def descargar_vehiculos_filtro(pendientes : List[VehiculosExcelResumen]):

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
    


    ##### Gestión GPS


@router.get("/getInfoTable")
async def Obtener_datos():

    # Ejecutar la consulta utilizando nuestra función
    datos = conn.obtener_informacion_gps()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id_gps" : fila[0],
                                "ppu": fila[1],
                                "razon_social": fila [2],
                                "rut": fila[3],
                                "region" : fila[4],
                                "gps" : fila[5],
                                "fec_instalacion":fila[6],
                                "oc_instalacion":fila[7],
                                "fec_baja": fila[8],
                                "oc_baja" : fila[9],
                                "monto": fila[10],
                                "descontado": fila[11],
                                "devuelto": fila[12],
                                "datos_varios": fila[13]

                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

@router.post("/oc_instalación")
async def actualizar_estado(oc_instalacion:str, id: int):
    try:
        conn.update_oc_instalacion_gps(oc_instalacion,id)
        print()
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.post("/oc_baja")
async def actualizar_estado(oc_baja:str, id: int):
    try:
        conn.update_oc_baja_gps(oc_baja,id)
        print()
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.post("/monto")
async def actualizar_estado(monto:str, id: int):
    try:
        conn.update_monto_gps(monto,id)
        print()
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.post("/descontado")
async def actualizar_estado(descontado:bool, id: int):
    try:
        conn.update_descontado_gps(descontado,id)
        print()
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@router.post("/devuelto")
async def actualizar_estado(devuelto:bool, id: int):
    try:
        conn.update_devuelto_gps(devuelto,id)
        print()
        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))



@router.get("/vehiculos/observaciones")
async def get_lista_vehiculo_observacion():
    datos = conn.listar_vehiculos_con_observaciones()
    return datos[0]


@router.get("/vehiculos/observaciones/descargar")
async def descargar_vehiculos_filtro():

    tupla = conn.listar_vehiculos_con_observaciones_descarga()

    nombre_filas = ( 'Patente', 'Razón Social', "Rut", 'Celular','Permiso Circulación', "SOAP", 
                     "Revisión Técnica","GPS")
    nombre_excel = f"Observacion_vehiculos"

    return excel.generar_excel_generico(tupla,nombre_filas,nombre_excel)


@router.get("/razon_social/at/descargar")
async def reporte_razon_soc_at():

    tupla = conn.reporte_razon_soc_at()

    nombre_filas = ( 'Razón Social', 'Rut Razón Social', "Estado", 'Documento Tributario','Giro', "Dirección", 
                     "Titular Cuenta","Tipo Cuenta","Rut Cta. Bancaria","N° Cta Bancaria","Banco",
                     "Mail","Representante Legal","Rut Representante Legal","Celular","Contrato","Id Hela")
    nombre_excel = f"Actualizacion AT - HELA"

    return excel.generar_excel_generico(tupla,nombre_filas,nombre_excel)


@router.get("/vehiculos/at/descargar")
async def reporte_vehiculos_at():

    tupla = conn.reporte_vehiculos_at()

    nombre_filas = ( "Patente", "Razón Social", "Estado", "Operación", "Región", "Comuna", 
    "Tipo Vehículo", "Marca", "Modelo", "Año", "Capcidad kg", "Capacidad m3",  "Fecha Venc. permiso", "Fecha Venc. Revisión", "Fecha Venc. SOAP", 
    "GPS", "Disponible", "Fecha Desvinculación",  "Fecha Inst. GPS", 
    "Fecha Desint. GPS", "Operación cop", "Id Hela")
    nombre_excel = f"Actualizacion AT - HELA"

    return excel.generar_excel_generico(tupla,nombre_filas,nombre_excel)



@router.get("/panel/vehiculos")
async def get_panel_vehiculos():
    datos_vehiculos = conn.panel_vehiculos()

    resultado_pv = {titulo.replace(' ','_') : cant for titulo, cant in datos_vehiculos}

    datos_observados = conn.panel_vehiculos_observados()

    resultado_obs = {titulo.replace(' ','_') : cant for titulo, cant in datos_observados}

    return {
        'Panel_vehiculos':resultado_pv,
        'Panel_vehiculos_obs': resultado_obs
    }


@router.get("/panel/colaboradores")
async def get_panel_colaboradores():
    datos = conn.panel_colaboradores()

    resultado_dict = {titulo.replace(' ','_') : cant for titulo, cant in datos}

    return resultado_dict


@router.get("/panel/tripulacion")
async def get_panel_tripulacion():
    datos = conn.panel_triplulacion()

    resultado_dict = {titulo.replace(' ','_') : cant for titulo, cant in datos}

    return resultado_dict


@router.get("/panel/vehiculos/observados")
async def get_panel_vehiculos_observados():
    datos = conn.panel_vehiculos_observados()

    resultado_dict = {titulo.replace(' ','_') : cant for titulo, cant in datos}

    return resultado_dict

@router.get("/selecciones/reclutamiento")
async def get_panel_colaboradores():
    datos = conn.datos_seleccionables_reclutamiento()
    resultado_dict = {titulo : cant for titulo, cant in datos}

    return resultado_dict


@router.get("/selecciones/tripulacion")
async def get_datos_seleccionables_tripulacion():
    datos = conn.datos_seleccionables_tripulacion()
    resultado_dict = {titulo : cant for titulo, cant in datos}

    return resultado_dict



@router.get("/datos/vehiculos")
async def datos_vehiculos():
    datos = conn.datos_vehiculos()
    resultado_dict = {titulo : cant for titulo, cant in datos}

    return resultado_dict

@router.get("/datos/razon_social")
async def datos_vehiculos():
    datos = conn.datos_razon_social()
    resultado_dict = {titulo : cant for titulo, cant in datos}

    return resultado_dict


@router.get("/drivers/observaciones")
async def get_listar_drivers_con_observaciones():
    datos = conn.listar_drivers_con_observaciones()

    return datos[0]


@router.get("/datos/reclutamiento")
async def get_datos_reclutamiento():
    datos = conn.obtener_datos_reclutamiento()

    return datos[0]



@router.get("/lista/comentarios")
async def get_lista_comentarios_recluta(id: int):
    datos = conn.lista_comentarios_recluta(id)

    return datos[0]


@router.post("/agregar/recluta")
async def agregar_nuevo_recluta(body : Reclutamiento):
    try:
        data = body.dict()
        conn.insert_nuevo_candidato(data)

        return {
            "message": "Candidato agregado correctamente",
            "razon" : "id_razon_social[0]"

        }
    except psycopg2.errors.UniqueViolation as error:
        # Manejar la excepción UniqueViolation específica
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: El rut de empresa {body.Rut_empresa} ya se encuentra registrado")

    except Exception as error:
        print(error)
        # Manejar otras excepciones
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error al agregar al nuevo recluta.")
    


@router.put("/actualizar/recluta")
async def actualizar_datos_colaborador(body : Reclutamiento):
    try:
        data = body.dict()
        conn.update_candidato(data)
        return {
            "message": "Recluta actualizado correctamente",

        }
    except psycopg2.errors.UniqueViolation as error:
        # Manejar la excepción UniqueViolation específica
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: El rut de empresa {body.Rut_empresa} ya se encuentra registrado")

    except Exception as error:
        print(error)
        # Manejar otras excepciones
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error al agregar al nuevo recluta.")
    


@router.get("/experiencia/comentario")
async def get_experiencia_comentario():
    datos = conn.datos_experiencia_comentario()

    return datos[0]


@router.post("/agregar/comentario")
async def agregar_comentario(body : ComentarioRecluta):
    try:
        data = body.dict()
        conn.insert_comentario_reclutamiento(data)

        return {
            "message": "Comentario agregado correctamente"

        }
    except psycopg2.errors.UniqueViolation as error:
        # Manejar la excepción UniqueViolation específica
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error: El rut de empresa {body.Rut_empresa} ya se encuentra registrado")

    except Exception as error:
        print(error)
        # Manejar otras excepciones
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Error al agregar al nuevo recluta.")
    
