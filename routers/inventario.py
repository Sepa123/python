from fastapi import APIRouter, status,HTTPException, Path

from database.models.mantenedores.personal import PersonalEquipo
from database.models.mantenedores.tipo import TipoDeEquipo
from database.models.mantenedores.equipo import DescripcionEquipo
from database.models.mantenedores.asignacion import AsignarEquipo
from database.models.mantenedores.departamento import DepartamentoInventario
from database.models.mantenedores.sucursal import SucursalInventario
from database.models.mantenedores.estado import EstadoInventario
from database.models.mantenedores.licencia import CrearLicencia


from database.schema.inventario.persona import crear_persona_schema, persona_equipo_schema
from database.schema.inventario.equipo import descripcion_equipo_schema, tipo_equipo_schema, lista_inventario_estado_schema, licencia_equipo_schema
from database.schema.inventario.sucursal import lista_sucursa_schema, lista_departamento_schema
##Conexiones
from database.client import reportesConnection

## en caso de generar un excel
from fastapi.responses import FileResponse
from openpyxl import Workbook
from openpyxl.styles import Font , PatternFill, Border ,Side

router = APIRouter(tags=["RSV"], prefix="/api/inventario-ti")

conn = reportesConnection()

@router.post("/asignacion")
async def asignar_equipo(body: AsignarEquipo):
    try:
        data = body.dict()
        conn.ingresar_equipo_asignado(data)
        return{
             "message": "Equipo asignado correctamente"
        }
    except Exception as e:
        print("error", data)
        raise HTTPException(status_code=422, detail=str(e))
    
@router.post("/licencia")
async def crear_licencia(body: CrearLicencia):
    try: 
        data=body.dict()
        conn.ingresar_licencia(data)
        return{
             "message": "Licencia creada correctamente"
        }
    except Exception as e:
        print("error", data)
        raise HTTPException(status_code=422, detail=str(e))

@router.post("/personal")
async def crear_personal(body: PersonalEquipo):
   
    try:
        data = body.dict()
        print("el try ", data)
        conn.ingresar_personal_equipo(data)
        return {
            "message": "Persona agregada correctamente"
        }
    except Exception as e:
        print("error", data)
        raise HTTPException(status_code=422, detail=str(e))
    
@router.post("/departamento")
async def crear_departamento(body: DepartamentoInventario):
    try: 
        data = body.dict()
        conn.ingresar_departamento(data)
        return{
            "message": "Departamento agregada correctamente"
        }
    except Exception as e:
        print("error", data)
        raise HTTPException(status_code=422, detail=str(e))
@router.post("/sucursal")
async def crear_sucursal(body: SucursalInventario):
    print(body)
    try:
        data = body.dict()
        print(data)
        conn.ingresar_sucursal(data)
        return{
            "message": "Sucursal agregada correctamente"
        }
    except Exception as e:
        print("error", data)
        raise HTTPException(status_code=422, detail=str(e))
    
@router.post("/estado")
async def crear_estado(body: EstadoInventario):
    try:
        data = body.dict()
        conn.ingresar_estado(data)
        return{
            "message": "Estado agregado correctamente"
        }
    except Exception as e:
        print("error", data)
        raise HTTPException(status_code=422, detail=str(e))
    
@router.post("/tipo-equipo")
async def crear_equipo(body: TipoDeEquipo ):
    try:
        data = body.dict()
        conn.ingresar_tipo_equipo(data)
        return{
            "message": "Se ha creado un equipo nuevo"
        }
    except Exception as e:
        raise HTTPException(status_code=422,detail=str(e))
    
@router.post("/equipo-descripcion")
async def agregar_descripcion_de_equipo(body: DescripcionEquipo):
    try: 
        data = body.dict()
        conn.agregar_descripcion_equipo(data)
        return{
            "message": "Se ha añadido la descripcion del equipo"
        }
    except Exception as e :
        raise HTTPException(status_code=422, detail=str(e))
    
@router.post("/asignar-equipo")
async def asignar_equipo_personal(body: AsignarEquipo):
    try: 
        data = body.dict()
        conn.asignar_equipo_personal(data)
        return{
            "message": "Equipo asignado"
        }
    except Exception as e:
        raise HTTPException(status_code=422,detail=str(e))
    

@router.get("/lista-licencia")
async def lista_licencias():
    result = conn.read_licencia_windows()
    return licencia_equipo_schema(result)

@router.get("/lista-personas")
async def listar_personas():
    result = conn.read_personas()
    return  crear_persona_schema(result)

@router.get("/lista-tipo-equipos")
async def listar_tipo_equipo():
    result = conn.read_tipo_equipo()
    return tipo_equipo_schema(result)

@router.get("/lista-descripcion-equipos")
async def listar_descripcion_equipo():
    result = conn.read_descripcion_equipo()
    return descripcion_equipo_schema(result)

@router.get("/lista-asignacion")
async def lista_equipo_asignado_personal():
    result = conn.read_asignaciones_personal()
    return persona_equipo_schema(result)

@router.get("/lista-sucursales")
async def lista_de_sucursales_por_inventario_ti():
    result = conn.read_sucursales_ti()
    return lista_sucursa_schema(result)

@router.get("/lista-departamentos")
async def lista_departamento_inventario():
    result = conn.read_departamento_inventario()
    return lista_departamento_schema(result)

@router.get("/lista-estados")
async def lista_estado_inventario():
    result = conn.read_estado_inventario()
    return lista_inventario_estado_schema(result)