from fastapi import APIRouter, status,HTTPException, Path, UploadFile, File

from database.models.mantenedores.personal import PersonalEquipo
from database.models.mantenedores.tipo import TipoDeEquipo
from database.models.mantenedores.equipo import DescripcionEquipo
from database.models.mantenedores.asignacion import AsignarEquipo
from database.models.mantenedores.devolucion import DevolucionEquipo
from database.models.mantenedores.departamento import DepartamentoInventario
from database.models.mantenedores.sucursal import SucursalInventario
from database.models.mantenedores.estado import EstadoInventario
from database.models.mantenedores.licencia import CrearLicencia
from database.models.mantenedores.pdf import GenerarPDF
from database.models.mantenedores.asignar_entrega_acta import AsignarEntregaActa

from database.schema.inventario.persona import crear_persona_schema, persona_equipo_schema, equipo_asignado_por_id_schema
from database.schema.inventario.equipo import lista_nr_equipo_schema,descripcion_equipo_schema, tipo_equipo_schema, lista_inventario_estado_schema, licencia_equipo_schema, folio_devolucion_schema, folio_entrega_schema
from database.schema.inventario.sucursal import lista_sucursa_schema, lista_departamento_schema
##Conexiones
from database.client import reportesConnection

## en caso de generar un excel
from fastapi.responses import FileResponse
from openpyxl import Workbook
from openpyxl.styles import Font , PatternFill, Border ,Side
from fpdf import FPDF
import json


router = APIRouter(tags=["RSV"], prefix="/api/inventario-ti")

conn = reportesConnection()

##CREAR INFORMACION A TRAVES DE FORM
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
    
    
@router.post("/generar_acta_entrega")
async def crear_acta(body: AsignarEntregaActa):
    try:
        data = body.dict()
        pdf_bytes = crear_pdf(body)
        return pdf_bytes 
    except Exception as e:
        print(data)
        raise HTTPException(status_code=422,detail=str(e))
    


def crear_pdf(data):
    # data = json.loads(body)
    pdf = FPDF('P','mm','Letter')
    pdf.add_page()
    # pdf.set_y(-15)
    pdf.set_font('helvetica', '', 14)
    pdf.set_top_margin(15)
    pdf.set_left_margin(16)
    # pdf.set_right_margin(1,4)
    pdf.image('logo.jpg',140,20,40)
    pdf.ln(30)
    pdf.cell(80,10,'Acta de entrega', align='L')
    pdf.cell(80,10, 'dsds', align='R', ln=1) 
    pdf.set_fill_color(192, 192, 192)
    pdf.set_font('helvetica', 'B', 12)
    pdf.cell(180,12, 'DATOS DEL USUARIO', ln=1, border=True, align='C', fill=True)
    pdf.set_font('helvetica', 'B', 12)
    pdf.cell(35,7, 'Nombre', border=True , align='C', fill=True)
    pdf.set_font('helvetica', '', 12)
    pdf.cell(55,7,data['nombres'], border=True , align='C')
    pdf.set_font('helvetica', 'B', 12)
    pdf.cell(35,7, 'Apellido', border=True , align='C', fill=True)
    pdf.set_font('helvetica', '', 12)
    pdf.cell(55,7,data['apellidos'], ln=1, border=True, align='C')
    pdf.set_font('helvetica', 'B', 12)
    pdf.cell(35,7, 'Cargo', border=True, align='C', fill=True)
    pdf.set_font('helvetica', '', 12)
    pdf.cell(55,7,data['cargo'], border=True, align='C')
    pdf.set_font('helvetica', 'B', 12)
    pdf.cell(35,7, 'RUT', border=True, align='C', fill=True)
    pdf.set_font('helvetica', '', 12)
    pdf.cell(55,7,data['rut'], ln=1, border=True, align='C')
    pdf.ln(10)
    pdf.set_font('helvetica', 'B', 12)
    pdf.cell(180,12, 'EQUIPO', border=True, ln=1 ,  align='C', fill=True)
    pdf.set_font('helvetica', 'B', 12)
    pdf.cell(40,7,'Modelo', border=True, align='C', fill=True)
    pdf.cell(100,7,'Descripción', border=True, align='C', fill=True)
    pdf.cell(40,7,'Serial', border=True, align='C', fill=True, ln=1)
    pdf.set_font('helvetica', '', 12)
    pdf.cell(40,7,data['marca'], border=True, align='C')
    pdf.cell(100,7,'modelo+serial+rom', border=True,  align='C')
    pdf.cell(40,7,data['serial'], border=True, ln=1, align='C')
    pdf.ln(10)
    pdf.cell(180,8,'Observaciones', border=True, ln=1, align='C', fill=True)
    pdf.cell(180,20, border=True, ln=1)
    pdf.set_font('helvetica', '', 10)
    pdf.multi_cell(180,5,""" 
                Certifico que los elementos detallados en el presente documento, me han sido entregados
                para mi cuidado y custodia con el propósito de cumplir con las tareas y asignaciones propias
                de mi cargo, siendo estos de mi única y exclusiva responsabilidad. Me comprometo a usar 
                correctamente los recursos, y solo para los fines establecidos, a no instalar ni permitir la 
                instalación de software por personal ajeno al grupo interno de trabajo. De igual forma me
                comprometo a devolver el equipo en las mismas condiciones y con los mismos accesorios 
                que me fue entregado, una vez mi vínculo laboral se dé por terminado. Está prohibido llevar 
                por cuenta propia a revisión técnica el Computador, para protección de la Garantía.
                ASIGNADO PARA TELETRABAJO""",border=True, ln=1)
    pdf.ln(10)
    pdf.set_font('helvetica', 'B', 12)
    pdf.cell(180,9,'ENTREGA DE EQUIPO', border=True, ln=1, align='C', fill=True)
    pdf.set_font('helvetica', 'B', 12)
    pdf.cell(90,7,'RECBIBE', border=True, align='C')
    pdf.cell(90,7,'ENTREGA', border=True, ln=1, align='C')
    pdf.set_font('helvetica', '', 12)
    pdf.cell(90,7, 'Nombre: ', border=True)
    pdf.cell(90,7, 'Nombre :'+ data['encargado_entrega'], border=True , ln=1)
    pdf.cell(90,7, 'Fecha :', border=True)
    pdf.cell(90,7, 'Fecha :' + data['fecha_entrega'], border=True , ln=1)
    pdf.cell(90,15, 'Firma : ', border=True)
    pdf.cell(90,15, 'Firma :', border=True , ln=1)
    pdf.output('pdf_1.pdf')
 
## MOSTRANDO LISTA DE LA INFORMACION ASIGNADA
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

@router.get("/folio/{folio}")
async def equipo_asignado_por_id(folio:str ):
    result = conn.encontrar_por_folio(folio)
    return persona_equipo_schema(result)

@router.get("/folio_entrega")
async def obtener_folio_entrega():
    result = conn.read_folio_entrega()
    return folio_entrega_schema(result)

@router.get("/folio_devolucion")
async def obtener_folio_devolucion():
    result = conn.read_folio_devolucion()
    return folio_devolucion_schema(result)

@router.get("/nr_equipo/{tipo}")
async def obtener_nr_equipo(tipo: int):
    result = conn.read_nr_equipo(tipo)
    return lista_nr_equipo_schema(result)

@router.get("/equipos-generales")
async def obtener_lista_equipos_generales():
    result = conn.read_equipos_general()
    return  descripcion_equipo_schema(result)
@router.get("/asignados/{id}")
async def obtener_asignados_por_id(id:int):
    result = conn.read_asignados_por_id(id)
    return equipo_asignado_por_id_schema(result)
#AGREGANDO DEVOLUCION DE EQUIPO ASIGNADO

@router.put("/actualizar/devolucion")
async def agregar_devolucion_equipo(  body: AsignarEquipo):
    try: 
        data = body.dict()
        conn.asignar_devolucion_equipo( data)
        return{
            "message": "Equipo devuelto"
        }
    except Exception as e:
        raise HTTPException(status_code=422,detail=str(e))

@router.put("/actualizar/tipo")
async def editar_tipo(body: TipoDeEquipo):
    try: 
        data = body.dict()
        conn.editar_tipo_equipo( data)
        return{
            "message": "Tipo editado"
        }
    except Exception as e:
        raise HTTPException(status_code=422,detail=str(e))

@router.put("/actualizar/departamento")
async def editar_departamento(body: DepartamentoInventario):
    try: 
        data = body.dict()
        conn.editar_departamento( data)
        return{
            "message": "Departamento editado"
        }
    except Exception as e:
        raise HTTPException(status_code=422,detail=str(e))  

@router.put("/actualizar/licencia")
async def editar_licencia(body: CrearLicencia):
    try:
        data = body.dict()
        conn.editar_licencia(data)
        return{
            "message": "Licencia editada"
        }
    except Exception as e:
        raise HTTPException(status_code=422,detail=str(e))
    
@router.put("/actualizar/sucursal")
async def editar_sucursal(body: SucursalInventario):
    try:
        data = body.dict()
        conn.editar_sucursal(data)
        return{
            "message": "Sucursal editada"
        }
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))
    
@router.put("/actualizar/estado")
async def editar_estado(body: EstadoInventario):
    try:
        data = body.dict()
        conn.editar_estado(data)
        return{
            "message": "Estado editado"
        }
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))
    
@router.put("/actualizar/descripcion-equipo")
async def editar_descripcion_equipo(body: DescripcionEquipo):
    try: 
        data = body.dict()
        conn.editar_descripcion_equipo(data)
        return{
            "message":"Descripcion de equipo editada"
        }
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))
    
@router.put("/actualizar/persona")
async def editar_persona(body: PersonalEquipo):
    try: 
        data = body.dict()
        conn.editar_persona(data)
        return{
            "message": "Persona editada"
        }
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))
    
@router.put("/crear-acta")
async def editar_crear_acta(body: AsignarEntregaActa):
    try:
        data = body.dict()
        conn.datos_acta_entrega(data)
        return{
            "message": "Persona editada"
        }
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))
