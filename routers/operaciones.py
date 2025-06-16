from fastapi import APIRouter, Request,status,UploadFile, File,HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from database.client import reportesConnection
from database.models.operaciones.modalidad_operacion import RazonSocial, updateApp
from database.models.operaciones.centro_operacion import CentroOperacion
from database.schema.operaciones.centro_operacion import centro_operacion_schema, centro_operacion_asignado_schema
from database.schema.operaciones.supervisores import datos_supervisores_schema
import psycopg2
import uvicorn

router = APIRouter(tags=["modalidadOperacion"],prefix="/api/operacion")
parametros_conexion = {

}

conn = reportesConnection()

def ejecutar_consulta(sql):
    try:
        conexion = psycopg2.connect(**parametros_conexion)
        cursor = conexion.cursor()
        cursor.execute(sql)
        filas = cursor.fetchall()
        cursor.close()
        conexion.close()
        return filas
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    


@router.get("/grupo_operaciones/campos")
async def get_campos_de_carga():
    datos = conn.datos_seleccion_grupo_operaciones()
    resultado_dict = {titulo : cant for titulo, cant in datos}

    return resultado_dict
    

@router.post("/agregar/razonSocial")
async def Agregar_RazonSocial(body: RazonSocial):
    try:
        data = body.dict()
        conn.agregar_razon_social(data)

        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
#### definicion de operaciones de modalidad operacion

@router.get("/def_operacion")
async def get_definicion_operacion():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.buscar_def_operacion()
    # Verificar si hay datos
    if datos:
        return datos[0]
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")
    


@router.get("/modalidad")
async def Obtener_datos():
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.buscar_modalidad_operacion()
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "created_at": fila[1],
                                "id_user": fila [2],
                                "ids:user": fila[3],
                                "nombre" : fila[4],
                                "description" : fila[5],
                                "Creation_date" : fila[6],
                                "update_date": fila[7],
                                "estado" : fila[8],
                                "cant_co" : fila[9],
                                "color" : fila[10],
                                "mod_operacion" : fila[11],
                                "mod_id" : fila[12],
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")


@router.get("/modalidad/buscar")
async def Obtener_datos(nombre: str):
     # Consulta SQL para obtener datos (por ejemplo)
    datos = conn.filtrar_modalidad_operacion_por_nombre(nombre)
    # Verificar si hay datos
    if datos:
        datos_formateados = [{
                                "id" : fila[0],
                                "created_at": fila[1],
                                "id_user": fila [2],
                                "ids:user": fila[3],
                                "nombre" : fila[4],
                                "description" : fila[5],
                                "Creation_date" : fila[6],
                                "update_date": fila[7],
                                "estado" : fila[8]
                            } 
                            for fila in datos]
        return datos_formateados
    else:
        raise HTTPException(status_code=404, detail="No se encontraron datos")


@router.delete("/modalidad/borrar")
async def eliminar_modalidad(id: str):
    conn.delete_modalidad_operacion(id)
    return {"message": f"Entrada con ID {id} eliminada correctamente"}


@router.post("/actualizar_estado")
async def actualizar_estado(ud: updateApp):
    try:
        conn.actualizar_modalidad_operacion(ud.estado,ud.id)

        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))



@router.post("/agregar/centro_operacion")
async def Agregar_centro_operacion(body: CentroOperacion):
    try:
        data = body.dict()
        conn.agregar_centro_operacion(data)

        return {"message": "Datos Ingresados Correctamente"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    


@router.get("/ver/centro_operacion")
async def get_centro_operacion(id_op : int):
     # Consulta SQL para obtener datos (por ejemplo)
    results = conn.mostrar_centros_operacion(id_op)


    return centro_operacion_schema(results)


@router.get("/ver/centros_operaciones")
async def get_centros_operacion():
     # Consulta SQL para obtener datos (por ejemplo)
    results = conn.mostrar_todos_centros_operacion()
    return centro_operacion_schema(results)


@router.get("/ver/centro_operacion/asignado")
async def get_centro_operacion_asigando_a_vehiculo(id_op : int, id_ppu : int):
     # Consulta SQL para obtener datos (por ejemplo)
    results = conn.mostrar_centros_operacion_asignado_a_vehiculo(id_op,id_ppu)
    return centro_operacion_asignado_schema(results)


@router.delete("/eliminar/centro_operacion") 
async def eliminar_centro_operacion(id : str):
     try:
          results = conn.eliminar_centro_operacion(id)
          if (results == 0): print("El centro operación asignada no existe")
          return { "message" : "Centro operación eliminado correctamente"}
     except:
          print("error en /eliminar/operacion/vehiculo")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")
     


@router.get("/ver/supervisores")
async def get_datos_supervisores_hela():
     # Consulta SQL para obtener datos (por ejemplo)
    results = conn.buscar_datos_supervisores_hela()
    return datos_supervisores_schema(results)




@router.put("/actualizar/servicio/operacion")
async def update_ruta_asignada(request : Request):
    try:
          body_p = await request.json()


          return { "message": "Ruta Actualizada Correctamente" }
    except:
          print("error en /actualizar/ruta_asignada")
          raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")

       
