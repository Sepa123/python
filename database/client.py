import psycopg2
from psycopg2.extras import execute_values, execute_batch

import codecs
from decouple import config
import os, sys, codecs
import subprocess
from database.queries import recuperar_query

# import datetime
# import pytz

comando = ["pm2", "restart", "0"]

### funcion que convierte los valores None en NULL para la insercion o update a la bd
def to_sql_value(value):
            if value is None:
                return "NULL"
            elif isinstance(value, str):
                return f"'{value}'"
            else:
                return str(value)

## decorador para en caso de que la base de datos de ty se desconecte
def reconnect_if_closed(func):
    
    def wrapper(self, *args, **kwargs):
        if self.conn is None or self.conn.closed:
            # print("la conexion esta cerrada")
            try:
                self.conn = psycopg2.connect(config("POSTGRES_DB_TR"))
            except psycopg2.OperationalError as err:
                print(err)
                print("Intentando reconectar...")
                subprocess.run(comando, shell=False)  # No tengo el valor de "comando", asegúrate de definirlo
        
        # else:
        #     print("base de datos conecatada")
        return func(self, *args, **kwargs)
    return wrapper


def reconnect_if_closed_postgres(func):
    
    def wrapper(self, *args, **kwargs):
        if self.conn is None or self.conn.closed:
            # print("la conexion esta cerrada")
            try:
                self.conn = psycopg2.connect(config("POSTGRES_DB_CARGA"))
            except psycopg2.OperationalError as err:
                print(err)
                print("Intentando reconectar...")
                subprocess.run(comando, shell=False)  # No tengo el valor de "comando", asegúrate de definirlo
        
        # else:
        #     print("base de datos conecatada")
        return func(self, *args, **kwargs)
    return wrapper

### Conexion usuario 

class UserConnection():
    conn = None
    def __init__(self) -> None:
        try:
            self.conn = psycopg2.connect(config("POSTGRES_DB_TR"))
        except psycopg2.OperationalError as err:
            print(err)
            print("Se conectara ???")
            # self.conn.close()
            #  self.conn = psycopg2.connect(config("POSTGRES_DB_TR"))
            subprocess.run(comando, shell=False)
        
    def __def__(self):
        self.conn.close()
    
    def conectar_bd(self):
        try:
            self.conn = psycopg2.connect(config("POSTGRES_DB_TR"))
        except psycopg2.OperationalError as err:
            print(err)
            print("Se conectara ???")
            self.conn.close()

    @reconnect_if_closed
    def write(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO public.users(username, password) VALUES(%(username)s, %(password)s);

            """,data)
        self.conn.commit()

    @reconnect_if_closed
    def read_all(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            select id, full_name ,mail,"password" ,active ,rol_id  from "user".users
            """)
            return cur.fetchall()
        
    @reconnect_if_closed    
    def read_roles(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT id, "name", description, extra_data, is_sub_rol
                FROM "user".rol;
            """)
            return cur.fetchall()
        
    @reconnect_if_closed
    def get_nombre_usuario(self, id_usuario : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT full_name from "user".users
            where id = {id_usuario}
            """)
            return cur.fetchone()
        
    @reconnect_if_closed
    def get_nombre_lista_usuarios(self, lista_usuarios : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT id, full_name from "user".users
            where id in ({lista_usuarios})
            """)
            return cur.fetchall()
        
    @reconnect_if_closed    
    def read_only_one(self, data):
        # self.conn = self.conectar_bd()
        # if self.conn.closed:
            # subprocess.run(comando, shell=False)

        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT id, full_name ,mail,"password" ,active ,rol_id, null as foto_perfil  FROM "user".users WHERE lower(mail) = lower(%(mail)s) and active=true
            """, data)
            return cur.fetchone()
        
# Especifica el timezone que deseas utilizar
timezone = 'America/Santiago'

# Configura las opciones de conexión con el timezone especificado
options = f'--timezone={timezone}'



class reportesConnection():
    conn = None
    def __init__(self) -> None:
        try:
            # print(config("POSTGRES_DB_CARGA"))
            self.conn = psycopg2.connect(config("POSTGRES_DB_CARGA"), options=options)
            # self.conn.encoding("")
            self.conn.autocommit = True
            self.conn.set_client_encoding("UTF-8")
        except psycopg2.OperationalError as err:
            print(err)
            # self.conn.close()
            subprocess.run(comando, shell=False)
            # self.conn = psycopg2.connect(config("POSTGRES_DB_CARGA"), options=options)
            # # self.conn.encoding("")
            # self.conn.autocommit = True
            # self.conn.set_client_encoding("UTF-8")
        
    def __def__(self):
        self.conn.close()

    def closeDB(self):
        self.conn.close()
    # Reporte historico 
    @reconnect_if_closed_postgres
    def read_reporte_historico_mensual(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT
            case
                When (to_char(CURRENT_DATE+ i,'d') = '1') then 'Domingo'
                When (to_char(CURRENT_DATE+ i,'d') = '2') then 'Lunes'
                When (to_char(CURRENT_DATE+ i,'d') = '3') then 'Martes'
                When (to_char(CURRENT_DATE+ i,'d') = '4') then 'Miercoles'
                When (to_char(CURRENT_DATE+ i,'d') = '5') then 'Jueves'
                When (to_char(CURRENT_DATE+ i,'d') = '6') then 'Viernes'
                When (to_char(CURRENT_DATE+ i,'d') = '7') then 'Sabado'
                End "Día",
            CURRENT_DATE+ i as "Fecha",
            (
                Select count(distinct(numero_guia))   from areati.ti_wms_carga_electrolux twce
                Where to_char(twce.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Electrolux",
            (
                Select count(distinct(entrega))  from areati.ti_wms_carga_easy easy
                Where to_char(easy.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Easy",
            (
                Select count(distinct(id_entrega)) from areati.ti_carga_easy_go_opl tienda
                Where to_char(tienda.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Easy OPL"
            From generate_series(date(date_trunc('month', current_date)) - CURRENT_DATE, 0 ) i
            order by 2 desc

            """)
            
            return cur.fetchall()
    
    @reconnect_if_closed_postgres
    def read_reporte_historico_hoy(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT
            case
                When (to_char(CURRENT_DATE+ i,'d') = '1') then 'Domingo'
                When (to_char(CURRENT_DATE+ i,'d') = '2') then 'Lunes'
                When (to_char(CURRENT_DATE+ i,'d') = '3') then 'Martes'
                When (to_char(CURRENT_DATE+ i,'d') = '4') then 'Miercoles'
                When (to_char(CURRENT_DATE+ i,'d') = '5') then 'Jueves'
                When (to_char(CURRENT_DATE+ i,'d') = '6') then 'Viernes'
                When (to_char(CURRENT_DATE+ i,'d') = '7') then 'Sabado'
                End "Día",
            CURRENT_DATE+ i as "Fecha",
            (
                Select count(distinct(numero_guia))  from areati.ti_wms_carga_electrolux twce
                Where to_char(twce.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Electrolux",
            (
                Select count(distinct(entrega)) from areati.ti_wms_carga_easy easy
                Where to_char(easy.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Easy",
            (
                Select count(distinct(id_entrega)) from areati.ti_carga_easy_go_opl tienda
                Where to_char(tienda.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Easy OPL"
            From generate_series(0,0) i
            order by 2 desc

            """)
            
            return cur.fetchall()
    
    @reconnect_if_closed_postgres
    def read_reporte_historico_anual(self):
        with self.conn.cursor() as cur:

            cur.execute("""
            SELECT
            case
                When (to_char(CURRENT_DATE+ i,'d') = '1') then 'Domingo'
                When (to_char(CURRENT_DATE+ i,'d') = '2') then 'Lunes'
                When (to_char(CURRENT_DATE+ i,'d') = '3') then 'Martes'
                When (to_char(CURRENT_DATE+ i,'d') = '4') then 'Miercoles'
                When (to_char(CURRENT_DATE+ i,'d') = '5') then 'Jueves'
                When (to_char(CURRENT_DATE+ i,'d') = '6') then 'Viernes'
                When (to_char(CURRENT_DATE+ i,'d') = '7') then 'Sabado'
                End "Día",
            CURRENT_DATE+ i as "Fecha",
            (
                Select count(distinct(numero_guia)) from areati.ti_wms_carga_electrolux twce
                Where to_char(twce.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Electrolux",
            (
                Select count(distinct(entrega)) from areati.ti_wms_carga_easy easy
                Where to_char(easy.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Easy",
            (
                Select count(distinct(id_entrega)) from areati.ti_wms_carga_tiendas tienda
                Where to_char(tienda.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Tiendas"
            From generate_series(date'2023-01-01'- CURRENT_DATE, 0 ) i

            """)

            return cur.fetchall()
    
    #Productos sin recepcion
    @reconnect_if_closed_postgres
    def read_productos_sin_recepcion(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """select * from hela.picking_anteriores();
                """
            )
            return cur.fetchall()

    
    #Cargas Verificadas y Total
    @reconnect_if_closed_postgres
    def read_cargas_easy(self):
        
        with self.conn.cursor() as cur:

            cur.execute("""
            ---------------- Presentacion por Pantalla ---------------------------------------
            select 'Total' as Indice, count(*) as cantidad from areati.ti_wms_carga_easy twce 
            where carton not like '%-%'
            and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            union all
            select 'Pistoleados' as Indice, count(*) as cantidad from areati.ti_wms_carga_easy twce 
            where carton not like '%-%'
            and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            and verified = true

            """)
            return cur.fetchall()
    # Quadminds
    @reconnect_if_closed_postgres
    def read_clientes(self):

        with self.conn.cursor() as cur:

            cur.execute("""
            -------------------------------------------------------------------------------------------------------------------------------------
    -- EASY
    -------------------------------------------------------------------------------------------------------------------------------------
    select       CAST(easy.entrega AS varchar) AS "Código de Cliente",     
                initcap(easy.nombre) AS "Nombre",
                CASE 
            WHEN substring(easy.direccion from '^\d') ~ '\d' then substring(initcap(easy.direccion) from '\d+[\w\s]+\d+')
            WHEN lower(easy.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
            regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easy.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
            else coalesce(substring(initcap(easy.direccion) from '^[^0-9]*[0-9]+'),initcap(easy.direccion))
            END "Calle y Número",
                case
                        when unaccent(lower(easy.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna))
                                                        )
                        )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(easy.comuna)) = unaccent(lower(oc2.comuna_name))
                                )
                end as "Ciudad",
                CASE
                        WHEN easy.region='XIII - Metropolitana' THEN 'Region Metropolitana'
                        WHEN easy.region='V - Valparaíso' THEN 'Valparaíso'
                        else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))
                END "Provincia/Estado",
                '' AS "Latitud",
                '' AS "Longitud", --7
                coalesce(easy.telefono,'0') AS "Teléfono con código de país",
                lower(easy.Correo) AS "Email",
                CAST(easy.entrega AS varchar) AS "Código de Pedido",   -- Agrupar Por
                easy.fecha_entrega AS "Fecha de Pedido",
                'E' AS "Operación E/R",
                (select string_agg(CAST(aux.carton AS varchar) , ' @ ') from areati.ti_wms_carga_easy aux
                where aux.entrega = easy.entrega) AS "Código de Producto",
                '(EASY) ' || (select string_agg(aux.descripcion , ' - ') from areati.ti_wms_carga_easy aux
                where aux.entrega = easy.entrega) AS "Descripción del Producto",
                (select count(*) from areati.ti_wms_carga_easy easy_a where easy_a.entrega = easy.entrega) AS "Cantidad de Producto", --15
                1 AS "Peso", --16
                1 AS "Volumen",
                1 AS "Dinero",
                '8' AS "Duración min",
                '09:00 - 21:00' AS "Ventana horaria 1",
                '' AS "Ventana horaria 2",
                'EASY CD' AS "Notas", -- 22
                CASE
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))='Region Metropolitana' THEN 'RM' || ' - ' ||
                        case
                            when unaccent(lower(easy.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select oc.comuna_name from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna))
                                                                )
                            )
                            else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                        where unaccent(lower(easy.comuna)) = unaccent(lower(oc2.comuna_name))
                                        )
                        end
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))='Valparaíso' THEN 'V - ' ||  initcap(easy.comuna)
                        else 'S/A'
                END "Agrupador",
                '' AS "Email de Remitentes",
                '' AS "Eliminar Pedido Si - No - Vacío",
                '' AS "Vehículo",
                '' AS "Habilidades"
    from areati.ti_wms_carga_easy easy
    where lower(easy.nombre) not like '%easy%'
    and (easy.estado=0 or (easy.estado=2 and easy.subestado not in (7,10,12,19,43,44,50,51,70,80))) and easy.estado not in (1,3)
    and to_char(easy.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
    group by easy.entrega,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20,21,22,23,24,25,26,27
                
                -------------------------------------------------------------------------------------------------------------------------------------
                -- ELECTROLUX
                -------------------------------------------------------------------------------------------------------------------------------------
                union all
                select eltx.identificador_contacto AS "Código de Cliente",
                        initcap(eltx.nombre_contacto) AS "Nombre",
                        CASE 
                            WHEN substring(initcap(split_part(eltx.direccion,',',1)) from '^\d') ~ '\d' then substring(initcap(split_part(eltx.direccion,',',1)) from '\d+[\w\s]+\d+')
                            WHEN lower(split_part(eltx.direccion,',',1)) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN 
                            -- cast(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.*$', ''), '\s+(\d+\D+\d+).*$', ' \1') AS varchar)
                            -- REPLACE(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', '')
                            regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '')
                            else coalesce(substring(initcap(split_part(eltx.direccion,',',1)) from '^[^0-9]*[0-9]+'),eltx.direccion)
                        END "Calle y Número",
                        --initcap(split_part(eltx.direccion,',',2)) AS "Ciudad",
                        --initcap(split_part(eltx.direccion,',',3)) AS "Provincia/Estado",
                        case
                        when unaccent(lower(eltx.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(eltx.comuna))
                                                        )
                        )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(eltx.comuna)) = unaccent(lower(oc2.comuna_name))
                                )
                end as "Ciudad",
                CASE
                        WHEN eltx.region='XIII - Metropolitana' THEN 'Region Metropolitana'
                        WHEN eltx.region='V - Valparaíso' THEN 'Valparaíso'
                        else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(eltx.comuna)))
                END "Provincia/Estado",
                        eltx.latitud AS "Latitud",
                        eltx.longitud AS "Longitud",
                        coalesce(eltx.telefono,'0') AS "Teléfono con código de país",
                        lower(eltx.email_contacto) AS "Email",
                        CAST (eltx.numero_guia AS varchar) AS "Código de Pedido",
                        eltx.fecha_min_entrega AS "Fecha de Pedido",
                        'E' AS "Operación E/R",
                        (select string_agg(CAST(aux.codigo_item AS varchar) , ' @ ') from areati.ti_wms_carga_electrolux aux
                            where aux.numero_guia = eltx.numero_guia) AS "Código de Producto",
                        -- CAST (eltx.numero_guia AS varchar) AS "Código de Producto",
                        --'(Electrolux) ' ||eltx.nombre_item AS "Descripción del Producto",
                            '(Electrolux) ' || (select string_agg(aux.nombre_item , ' - ') from areati.ti_wms_carga_electrolux aux
                            where aux.numero_guia = eltx.numero_guia) AS "Descripción del Producto",
                        --eltx.cantidad AS "Cantidad de Producto",
                            (select count(*) from areati.ti_wms_carga_electrolux eltx_a where eltx_a.numero_guia = eltx.numero_guia) as "Cantidad de Producto",
                        1 AS "Peso",
                        1 AS "Volumen",
                        1 AS "Dinero",
                        '8' AS "Duración min",
                        '09:00 - 21:00' AS "Ventana horaria 1",
                        '' AS "Ventana horaria 2",
                        coalesce((select 'Electrolux - ' || se.name from areati.subestado_entregas se where eltx.subestado=se.code),'Electrolux') AS "Notas",
                        --'RM - ' ||  initcap(eltx.comuna)  AS agrupador,
                                    CASE
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(eltx.comuna)))='Region Metropolitana' THEN 'RM' || ' - ' ||
                        case
                            when unaccent(lower(eltx.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select oc.comuna_name from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(eltx.comuna))
                                                                )
                            )
                            else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                        where unaccent(lower(eltx.comuna)) = unaccent(lower(oc2.comuna_name))
                                        )
                        end
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(eltx.comuna)))='Valparaíso' THEN 'V - ' ||  initcap(eltx.comuna)
                        else 'S/A'
                END "Agrupador",
                        '' AS "Email de Remitentes",
                        '' AS "Eliminar Pedido Si - No - Vacío",
                        '' AS "Vehículo",
                    '' AS "Habilidades"
                from areati.ti_wms_carga_electrolux eltx
                where to_char(eltx.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
                    and (eltx.estado=0 or (eltx.estado=2 and eltx.subestado not in (7,10,12,19,43,44,50,51,70,80))) and eltx.estado not in (1,3)
                    group by eltx.numero_guia,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20,21,22,23,24,25,26,27,1
                    
                -------------------------------------------------------------------------------------------------------------------------------------
                -- SPORTEX
                -------------------------------------------------------------------------------------------------------------------------------------
                union all
                select      twcs.id_sportex AS "Código de Cliente",
                        initcap(twcs.cliente) AS "Nombre",
                        CASE 
                            WHEN substring(initcap(replace(twcs.direccion,',','')) from '^\d') ~ '\d' then substring(initcap(replace(twcs.direccion,',','')) from '\d+[\w\s]+\d+')
                            WHEN lower(twcs.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN 
                            regexp_replace(regexp_replace(initcap(twcs.direccion), ',.*$', ''), '\s+(\d+\D+\d+).*$', ' \1')
                            else substring(initcap(replace(twcs.direccion,',','')) from '^[^0-9]*[0-9]+')
                        END "Calle y Número",
                        --initcap(COALESCE(tcr.region, 'Region Metropolitana')) AS "Ciudad",
                        --initcap(twcs.comuna)  AS "Provincia/Estado",
                                    case
                        when unaccent(lower(twcs.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twcs.comuna))
                                                        )
                        )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(twcs.comuna)) = unaccent(lower(oc2.comuna_name))
                                )
                            end as "Ciudad",
                            CASE
                        WHEN twcs.comuna='XIII - Metropolitana' THEN 'Region Metropolitana'
                        WHEN twcs.comuna='V - Valparaíso' THEN 'Valparaíso'
                        else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(twcs.comuna)))
                            END "Provincia/Estado",
                        '' AS "Latitud",
                        '' AS "Longitud",
                        coalesce(twcs.fono,'0') AS "Teléfono con código de país",
                        lower(twcs.correo) AS "Email",
                        CAST (twcs.id_sportex AS varchar) AS "Código de Pedido",
                        twcs.fecha_entrega AS "Fecha de Pedido",
                        'E' AS "Operación E/R",
                        twcs.id_sportex AS "Código de Producto",
                        '(Sportex) ' || coalesce(twcs.marca,'Sin Marca') AS "Descripción del Producto", 
                        1 AS "Cantidad de Producto",
                        1 AS "Peso",
                        1 AS "Volumen",
                        1 AS "Dinero",
                        '8' AS "Duración min",
                        '09:00 - 21:00' AS "Ventana horaria 1",
                        '' AS "Ventana horaria 2",
                        coalesce((select 'Sportex - ' || se.name from areati.subestado_entregas se where twcs.subestado=se.code),'Sportex') AS "Notas",
                        CASE
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(twcs.comuna)))='Region Metropolitana' THEN 'RM' || ' - ' ||
                        case
                            when unaccent(lower(twcs.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select oc.comuna_name from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twcs.comuna))
                                                                )
                            )
                            else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                        where unaccent(lower(twcs.comuna)) = unaccent(lower(oc2.comuna_name))
                                        )
                        end
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(twcs.comuna)))='Valparaíso' THEN 'V - ' ||  initcap(twcs.comuna)
                        else 'S/A'
                END "Agrupador",
                        '' AS "Email de Remitentes",
                        '' AS "Eliminar Pedido Si - No - Vacío",
                        '' AS "Vehículo",
                        '' AS "Habilidades"
                from areati.ti_wms_carga_sportex twcs
                left join ti_comuna_region tcr on
                    translate(lower(twcs.comuna),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
                where to_char(twcs.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
                and (twcs.estado=0 or (twcs.estado=2 and twcs.subestado not in (7,10,12,19,43,44,50,51,70,80))) and twcs.estado not in (1,3)

                -------------------------------------------------------------------------------------------------------------------------------------
                -- Easy OPL
                -------------------------------------------------------------------------------------------------------------------------------------
                union all  
                select  easygo.rut_cliente AS "Código de Cliente",
                        initcap(easygo.nombre_cliente) AS "Nombre",
                        initcap(easygo.direc_despacho) AS "Calle y Número",
                        case
                        when unaccent(lower(easygo.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easygo.comuna_despacho))
                                                        )
                        )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(easygo.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                                )
                            end as "Ciudad",

                        --initcap(COALESCE(tcr.region, 'Region Metropolitana')) AS "Ciudad",
                        --initcap(easygo.comuna_despacho)  AS "Provincia/Estado",
                            CASE
                            WHEN easygo.comuna_despacho='XIII - Metropolitana' THEN 'Región Metropolitana'
                            WHEN easygo.comuna_despacho='V - Valparaíso' THEN 'Valparaíso'
                            else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easygo.comuna_despacho)))
                            END "Provincia/Estado",
                        '' AS "Latitud",
                        '' AS "Longitud",
                        coalesce(easygo.fono_cliente ,'0') AS "Teléfono con código de país",
                        lower(easygo.correo_cliente) AS "Email",
                        CAST (easygo.suborden AS varchar) AS "Código de Pedido",
                        easygo.fec_compromiso AS "Fecha de Pedido",
                        'E' AS "Operación E/R",
                        --easygo.id_entrega AS "Código de Producto",
                        (select string_agg(CAST(aux.codigo_sku AS varchar) , ' @ ') from areati.ti_carga_easy_go_opl aux
                            where aux.suborden = easygo.suborden) AS "Código de Producto",
                            
                        '(Easy OPL) ' || (select string_agg(aux.descripcion , ' - ') from areati.ti_carga_easy_go_opl aux
                            where aux.suborden = easygo.suborden) AS "Descripción del Producto",
                            (select count(*) from areati.ti_carga_easy_go_opl easy_a where easy_a.suborden = easygo.suborden) AS "Cantidad de Producto",
                        1 AS "Peso",
                        1 AS "Volumen",
                        1 AS "Dinero",
                        '8' AS "Duración min",
                        '09:00 - 21:00' AS "Ventana horaria 1",
                        '' AS "Ventana horaria 2",
                        coalesce((select 'Easy OPL - ' || se.name from areati.subestado_entregas se where easygo.subestado=se.code),'Easy OPL') AS "Notas",
                        CASE
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easygo.comuna_despacho)))='Region Metropolitana' THEN 'RM' || ' - ' ||
                        case
                            when unaccent(lower(easygo.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select oc.comuna_name from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easygo.comuna_despacho))
                                                                )
                            )
                            else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                        where unaccent(lower(easygo.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                                        )
                        end
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easygo.comuna_despacho)))='Valparaíso' THEN 'V - ' ||  initcap(easygo.comuna_despacho)
                        else 'S/A'
                END "Agrupador",
                        '' AS "Email de Remitentes",
                        '' AS "Eliminar Pedido Si - No - Vacío",
                        '' AS "Vehículo",
                        '' AS "Habilidades"
                from areati.ti_carga_easy_go_opl easygo
                left join ti_comuna_region tcr on
                    translate(lower(easygo.comuna_despacho),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
                where to_char(easygo.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
                and (easygo.estado=0 or (easygo.estado=2 and easygo.subestado not in (7,10,12,19,43,44,50,51,70,80))) and easygo.estado not in (1,3)
                group by easygo.suborden,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20,21,22,23,24,25,26,27,1

            """)
            return cur.fetchall()
    
    @reconnect_if_closed_postgres
    def read_reporte_quadmind_fecha_compromiso(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            -------------------------------------------------------------------------------------------------------------------------------------
            -- EASY
            -------------------------------------------------------------------------------------------------------------------------------------
            select       CAST(easy.entrega AS varchar) AS "Código de Cliente",     
                        initcap(easy.nombre) AS "Nombre",
                        CASE 
            WHEN substring(easy.direccion from '^\d') ~ '\d' then substring(initcap(easy.direccion) from '\d+[\w\s]+\d+')
            WHEN lower(easy.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
            regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easy.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
            else coalesce(substring(initcap(easy.direccion) from '^[^0-9]*[0-9]+'),initcap(easy.direccion))
            END "Calle y Número",
                case
                        when unaccent(lower(easy.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna))
                                                        )
                        )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(easy.comuna)) = unaccent(lower(oc2.comuna_name))
                                )
                end as "Ciudad",
                CASE
                        WHEN easy.region='XIII - Metropolitana' THEN 'Region Metropolitana'
                        WHEN easy.region='V - Valparaíso' THEN 'Valparaíso'
                        else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))
                END "Provincia/Estado",
                '' AS "Latitud",
                '' AS "Longitud", --7
                coalesce(easy.telefono,'0') AS "Teléfono con código de país",
                lower(easy.Correo) AS "Email",
                CAST(easy.entrega AS varchar) AS "Código de Pedido",   -- Agrupar Por
                easy.fecha_entrega AS "Fecha de Pedido",
                'E' AS "Operación E/R",
                (select string_agg(CAST(aux.carton AS varchar) , ' @ ') from areati.ti_wms_carga_easy aux
                where aux.entrega = easy.entrega) AS "Código de Producto",
                '(EASY) ' || (select string_agg(aux.descripcion , ' - ') from areati.ti_wms_carga_easy aux
                where aux.entrega = easy.entrega) AS "Descripción del Producto",
                (select count(*) from areati.ti_wms_carga_easy easy_a where easy_a.entrega = easy.entrega) AS "Cantidad de Producto", --15
                1 AS "Peso", --16
                1 AS "Volumen",
                1 AS "Dinero",
                '8' AS "Duración min",
                '09:00 - 21:00' AS "Ventana horaria 1",
                '' AS "Ventana horaria 2",
                'EASY CD' AS "Notas", -- 22
                CASE
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))='Region Metropolitana' THEN 'RM' || ' - ' ||
                        case
                            when unaccent(lower(easy.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select oc.comuna_name from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna))
                                                                )
                            )
                            else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                        where unaccent(lower(easy.comuna)) = unaccent(lower(oc2.comuna_name))
                                        )
                        end
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))='Valparaíso' THEN 'V - ' ||  initcap(easy.comuna)
                        else 'S/A'
                END "Agrupador",
                '' AS "Email de Remitentes",
                '' AS "Eliminar Pedido Si - No - Vacío",
                '' AS "Vehículo",
                '' AS "Habilidades"
    from areati.ti_wms_carga_easy easy
    where lower(easy.nombre) not like '%easy%'
    and (easy.estado=0 or (easy.estado=2 and easy.subestado not in (7,10,12,19,43,44,50,51,70,80))) and easy.estado not in (1,3)
    and to_char(easy.fecha_entrega,'yyyymmdd')<=to_char(current_date,'yyyymmdd')
    group by easy.entrega,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20,21,22,23,24,25,26,27
                
                -------------------------------------------------------------------------------------------------------------------------------------
                -- ELECTROLUX
                -------------------------------------------------------------------------------------------------------------------------------------
                union all
                select eltx.identificador_contacto AS "Código de Cliente",
                        initcap(eltx.nombre_contacto) AS "Nombre",
                        CASE 
                            WHEN substring(initcap(split_part(eltx.direccion,',',1)) from '^\d') ~ '\d' then substring(initcap(split_part(eltx.direccion,',',1)) from '\d+[\w\s]+\d+')
                            WHEN lower(split_part(eltx.direccion,',',1)) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN 
                            -- cast(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.*$', ''), '\s+(\d+\D+\d+).*$', ' \1') AS varchar)
                            -- REPLACE(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', '')
                            regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '')
                            else coalesce(substring(initcap(split_part(eltx.direccion,',',1)) from '^[^0-9]*[0-9]+'),eltx.direccion)
                        END "Calle y Número",
                        --initcap(split_part(eltx.direccion,',',2)) AS "Ciudad",
                        --initcap(split_part(eltx.direccion,',',3)) AS "Provincia/Estado",
                        case
                        when unaccent(lower(eltx.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(eltx.comuna))
                                                        )
                        )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(eltx.comuna)) = unaccent(lower(oc2.comuna_name))
                                )
                end as "Ciudad",
                CASE
                        WHEN eltx.region='XIII - Metropolitana' THEN 'Region Metropolitana'
                        WHEN eltx.region='V - Valparaíso' THEN 'Valparaíso'
                        else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(eltx.comuna)))
                END "Provincia/Estado",
                        eltx.latitud AS "Latitud",
                        eltx.longitud AS "Longitud",
                        coalesce(eltx.telefono,'0') AS "Teléfono con código de país",
                        lower(eltx.email_contacto) AS "Email",
                        CAST (eltx.numero_guia AS varchar) AS "Código de Pedido",
                        eltx.fecha_min_entrega AS "Fecha de Pedido",
                        'E' AS "Operación E/R",
                        (select string_agg(CAST(aux.codigo_item AS varchar) , ' @ ') from areati.ti_wms_carga_electrolux aux
                            where aux.numero_guia = eltx.numero_guia) AS "Código de Producto",
                        -- CAST (eltx.numero_guia AS varchar) AS "Código de Producto",
                        --'(Electrolux) ' ||eltx.nombre_item AS "Descripción del Producto",
                            '(Electrolux) ' || (select string_agg(aux.nombre_item , ' - ') from areati.ti_wms_carga_electrolux aux
                            where aux.numero_guia = eltx.numero_guia) AS "Descripción del Producto",
                        --eltx.cantidad AS "Cantidad de Producto",
                            (select count(*) from areati.ti_wms_carga_electrolux eltx_a where eltx_a.numero_guia = eltx.numero_guia) as "Cantidad de Producto",
                        1 AS "Peso",
                        1 AS "Volumen",
                        1 AS "Dinero",
                        '8' AS "Duración min",
                        '09:00 - 21:00' AS "Ventana horaria 1",
                        '' AS "Ventana horaria 2",
                        coalesce((select 'Electrolux - ' || se.name from areati.subestado_entregas se where eltx.subestado=se.code),'Electrolux') AS "Notas",
                        --'RM - ' ||  initcap(eltx.comuna)  AS agrupador,
                                    CASE
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(eltx.comuna)))='Region Metropolitana' THEN 'RM' || ' - ' ||
                        case
                            when unaccent(lower(eltx.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select oc.comuna_name from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(eltx.comuna))
                                                                )
                            )
                            else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                        where unaccent(lower(eltx.comuna)) = unaccent(lower(oc2.comuna_name))
                                        )
                        end
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(eltx.comuna)))='Valparaíso' THEN 'V - ' ||  initcap(eltx.comuna)
                        else 'S/A'
                END "Agrupador",
                        '' AS "Email de Remitentes",
                        '' AS "Eliminar Pedido Si - No - Vacío",
                        '' AS "Vehículo",
                    '' AS "Habilidades"
                from areati.ti_wms_carga_electrolux eltx
                where to_char(eltx.fecha_min_entrega,'yyyymmdd')<=to_char(current_date,'yyyymmdd')
                    and (eltx.estado=0 or (eltx.estado=2 and eltx.subestado not in (7,10,12,19,43,44,50,51,70,80))) and eltx.estado not in (1,3)
                    group by eltx.numero_guia,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20,21,22,23,24,25,26,27,1
                    
                -------------------------------------------------------------------------------------------------------------------------------------
                -- SPORTEX
                -------------------------------------------------------------------------------------------------------------------------------------
                union all
                select      twcs.id_sportex AS "Código de Cliente",
                        initcap(twcs.cliente) AS "Nombre",
                        CASE 
                            WHEN substring(initcap(replace(twcs.direccion,',','')) from '^\d') ~ '\d' then substring(initcap(replace(twcs.direccion,',','')) from '\d+[\w\s]+\d+')
                            WHEN lower(twcs.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN 
                            regexp_replace(regexp_replace(initcap(twcs.direccion), ',.*$', ''), '\s+(\d+\D+\d+).*$', ' \1')
                            else substring(initcap(replace(twcs.direccion,',','')) from '^[^0-9]*[0-9]+')
                        END "Calle y Número",
                        --initcap(COALESCE(tcr.region, 'Region Metropolitana')) AS "Ciudad",
                        --initcap(twcs.comuna)  AS "Provincia/Estado",
                                    case
                        when unaccent(lower(twcs.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twcs.comuna))
                                                        )
                        )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(twcs.comuna)) = unaccent(lower(oc2.comuna_name))
                                )
                            end as "Ciudad",
                            CASE
                        WHEN twcs.comuna='XIII - Metropolitana' THEN 'Region Metropolitana'
                        WHEN twcs.comuna='V - Valparaíso' THEN 'Valparaíso'
                        else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(twcs.comuna)))
                            END "Provincia/Estado",
                        '' AS "Latitud",
                        '' AS "Longitud",
                        coalesce(twcs.fono,'0') AS "Teléfono con código de país",
                        lower(twcs.correo) AS "Email",
                        CAST (twcs.id_sportex AS varchar) AS "Código de Pedido",
                        twcs.fecha_entrega AS "Fecha de Pedido",
                        'E' AS "Operación E/R",
                        twcs.id_sportex AS "Código de Producto",
                        '(Sportex) ' || coalesce(twcs.marca,'Sin Marca') AS "Descripción del Producto", 
                        1 AS "Cantidad de Producto",
                        1 AS "Peso",
                        1 AS "Volumen",
                        1 AS "Dinero",
                        '8' AS "Duración min",
                        '09:00 - 21:00' AS "Ventana horaria 1",
                        '' AS "Ventana horaria 2",
                        coalesce((select 'Sportex - ' || se.name from areati.subestado_entregas se where twcs.subestado=se.code),'Sportex') AS "Notas",
                        CASE
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(twcs.comuna)))='Region Metropolitana' THEN 'RM' || ' - ' ||
                        case
                            when unaccent(lower(twcs.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select oc.comuna_name from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twcs.comuna))
                                                                )
                            )
                            else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                        where unaccent(lower(twcs.comuna)) = unaccent(lower(oc2.comuna_name))
                                        )
                        end
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(twcs.comuna)))='Valparaíso' THEN 'V - ' ||  initcap(twcs.comuna)
                        else 'S/A'
                END "Agrupador",
                        '' AS "Email de Remitentes",
                        '' AS "Eliminar Pedido Si - No - Vacío",
                        '' AS "Vehículo",
                        '' AS "Habilidades"
                from areati.ti_wms_carga_sportex twcs
                left join ti_comuna_region tcr on
                    translate(lower(twcs.comuna),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
                where to_char(twcs.fecha_entrega,'yyyymmdd')<=to_char(current_date,'yyyymmdd')
                and (twcs.estado=0 or (twcs.estado=2 and twcs.subestado not in (7,10,12,19,43,44,50,51,70,80))) and twcs.estado not in (1,3)

                -------------------------------------------------------------------------------------------------------------------------------------
                -- Easy OPL
                -------------------------------------------------------------------------------------------------------------------------------------
                union all  
                select  easygo.rut_cliente AS "Código de Cliente",
                        initcap(easygo.nombre_cliente) AS "Nombre",
                        initcap(easygo.direc_despacho) AS "Calle y Número",
                        case
                        when unaccent(lower(easygo.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easygo.comuna_despacho))
                                                        )
                        )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(easygo.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                                )
                            end as "Ciudad",

                        --initcap(COALESCE(tcr.region, 'Region Metropolitana')) AS "Ciudad",
                        --initcap(easygo.comuna_despacho)  AS "Provincia/Estado",
                            CASE
                            WHEN easygo.comuna_despacho='XIII - Metropolitana' THEN 'Región Metropolitana'
                            WHEN easygo.comuna_despacho='V - Valparaíso' THEN 'Valparaíso'
                            else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easygo.comuna_despacho)))
                            END "Provincia/Estado",
                        '' AS "Latitud",
                        '' AS "Longitud",
                        coalesce(easygo.fono_cliente ,'0') AS "Teléfono con código de país",
                        lower(easygo.correo_cliente) AS "Email",
                        CAST (easygo.suborden AS varchar) AS "Código de Pedido",
                        easygo.fec_compromiso AS "Fecha de Pedido",
                        'E' AS "Operación E/R",
                        --easygo.id_entrega AS "Código de Producto",
                        (select string_agg(CAST(aux.codigo_sku AS varchar) , ' @ ') from areati.ti_carga_easy_go_opl aux
                            where aux.suborden = easygo.suborden) AS "Código de Producto",
                            
                        '(Easy OPL) ' || (select string_agg(aux.descripcion , ' - ') from areati.ti_carga_easy_go_opl aux
                            where aux.suborden = easygo.suborden) AS "Descripción del Producto",
                            (select count(*) from areati.ti_carga_easy_go_opl easy_a where easy_a.suborden = easygo.suborden) AS "Cantidad de Producto",
                        1 AS "Peso",
                        1 AS "Volumen",
                        1 AS "Dinero",
                        '8' AS "Duración min",
                        '09:00 - 21:00' AS "Ventana horaria 1",
                        '' AS "Ventana horaria 2",
                        coalesce((select 'Easy OPL - ' || se.name from areati.subestado_entregas se where easygo.subestado=se.code),'Easy OPL') AS "Notas",
                        CASE
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easygo.comuna_despacho)))='Region Metropolitana' THEN 'RM' || ' - ' ||
                        case
                            when unaccent(lower(easygo.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select oc.comuna_name from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easygo.comuna_despacho))
                                                                )
                            )
                            else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                        where unaccent(lower(easygo.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                                        )
                        end
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easygo.comuna_despacho)))='Valparaíso' THEN 'V - ' ||  initcap(easygo.comuna_despacho)
                        else 'S/A'
                END "Agrupador",
                        '' AS "Email de Remitentes",
                        '' AS "Eliminar Pedido Si - No - Vacío",
                        '' AS "Vehículo",
                        '' AS "Habilidades"
                from areati.ti_carga_easy_go_opl easygo
                left join ti_comuna_region tcr on
                    translate(lower(easygo.comuna_despacho),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
                where to_char(easygo.fec_compromiso,'yyyymmdd')<=to_char(current_date,'yyyymmdd')
                and (easygo.estado=0 or (easygo.estado=2 and easygo.subestado not in (7,10,12,19,43,44,50,51,70,80))) and easygo.estado not in (1,3)
                group by easygo.suborden,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20,21,22,23,24,25,26,27,1
                    

            """)
            return cur.fetchall()
    
    @reconnect_if_closed_postgres
    def read_resumen_quadmind_tamano(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                -------------------------------------------------------------------------------------------------------------------------------------
                -- EASY
                -------------------------------------------------------------------------------------------------------------------------------------
                select       CAST(easy.entrega AS varchar) AS "Código de Cliente",     
                            initcap(easy.nombre) AS "Nombre",
                            CASE 
                        WHEN substring(easy.direccion from '^\d') ~ '\d' then substring(initcap(easy.direccion) from '\d+[\w\s]+\d+')
                        WHEN lower(easy.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                        regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easy.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                        else coalesce(substring(initcap(easy.direccion) from '^[^0-9]*[0-9]+'),initcap(easy.direccion))
                        END "Calle y Número",
                            case
                                    when unaccent(lower(easy.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                    (select oc.comuna_name from public.op_comunas oc 
                                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                            where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna))
                                                                    )
                                    )
                                    else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                            where unaccent(lower(easy.comuna)) = unaccent(lower(oc2.comuna_name))
                                            )
                            end as "Ciudad",
                            CASE
                                    WHEN easy.region='XIII - Metropolitana' THEN 'Region Metropolitana'
                                    WHEN easy.region='V - Valparaíso' THEN 'Valparaíso'
                                    else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))
                            END "Provincia/Estado",
                            '' AS "Latitud",
                            '' AS "Longitud", --7
                            coalesce(easy.telefono,'0') AS "Teléfono con código de país",
                            lower(easy.Correo) AS "Email",
                            CAST(easy.entrega AS varchar) AS "Código de Pedido",   -- Agrupar Por
                            easy.fecha_entrega AS "Fecha de Pedido",
                            'E' AS "Operación E/R",
                            (select string_agg(CAST(aux.carton AS varchar) , ' @ ') from areati.ti_wms_carga_easy aux
                            where aux.entrega = easy.entrega) AS "Código de Producto",
                            '(EASY) ' || (select string_agg(aux.descripcion , ' - ') from areati.ti_wms_carga_easy aux
                            where aux.entrega = easy.entrega) AS "Descripción del Producto",
                            (select count(*) from areati.ti_wms_carga_easy easy_a where easy_a.entrega = easy.entrega) AS "Cantidad de Producto", --15
                            1 AS "Peso", --16
                            1 AS "Volumen",
                            1 AS "Dinero",
                            '8' AS "Duración min",
                            '09:00 - 21:00' AS "Ventana horaria 1",
                            '' AS "Ventana horaria 2",
                            'EASY CD' AS "Notas", -- 22
                            CASE
                                    WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))='Region Metropolitana' 
                                    THEN 'RM' || ' - ' || coalesce (tts.tamano,'?')
                                    WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))='Valparaíso' THEN 'V - ' ||  initcap(easy.comuna)
                                    else 'S/A'
                            END "Agrupador",
                            '' AS "Email de Remitentes",
                            '' AS "Eliminar Pedido Si - No - Vacío",
                            '' AS "Vehículo",
                            '' AS "Habilidades"
                from areati.ti_wms_carga_easy easy
                left join public.ti_tamano_sku tts on tts.sku = cast(easy.producto as text)
                where lower(easy.nombre) not like '%easy%'
                and (easy.estado=0 or (easy.estado=2 and easy.subestado not in (7,10,12,19,43,44,50,51,70,80))) and easy.estado not in (1,3)
                and to_char(easy.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
                group by easy.entrega,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20,21,22,23,24,25,26,27
                -------------------------------------------------------------------------------------------------------------------------------------
                -- ELECTROLUX
                -------------------------------------------------------------------------------------------------------------------------------------
                union all
                select eltx.identificador_contacto AS "Código de Cliente",
                        initcap(eltx.nombre_contacto) AS "Nombre",
                        CASE 
                WHEN substring(initcap(split_part(eltx.direccion,',',1)) from '^\d') ~ '\d' then substring(initcap(split_part(eltx.direccion,',',1)) from '\d+[\w\s]+\d+')
                WHEN lower(split_part(eltx.direccion,',',1)) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN 
                -- cast(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.*$', ''), '\s+(\d+\D+\d+).*$', ' \1') AS varchar)
                -- REPLACE(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', '')
                regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '')
                else coalesce(substring(initcap(split_part(eltx.direccion,',',1)) from '^[^0-9]*[0-9]+'),eltx.direccion)
                        END "Calle y Número",
                        --initcap(split_part(eltx.direccion,',',2)) AS "Ciudad",
                        --initcap(split_part(eltx.direccion,',',3)) AS "Provincia/Estado",
                        case
                        when unaccent(lower(eltx.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(eltx.comuna))
                    )
                        )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                    where unaccent(lower(eltx.comuna)) = unaccent(lower(oc2.comuna_name))
                    )
                end as "Ciudad",
                CASE
                        WHEN eltx.region='XIII - Metropolitana' THEN 'Region Metropolitana'
                        WHEN eltx.region='V - Valparaíso' THEN 'Valparaíso'
                        else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(eltx.comuna)))
                END "Provincia/Estado",
                        eltx.latitud AS "Latitud",
                        eltx.longitud AS "Longitud",
                        coalesce(eltx.telefono,'0') AS "Teléfono con código de país",
                        lower(eltx.email_contacto) AS "Email",
                        CAST (eltx.numero_guia AS varchar) AS "Código de Pedido",
                        eltx.fecha_min_entrega AS "Fecha de Pedido",
                        'E' AS "Operación E/R",
                        (select string_agg(CAST(aux.codigo_item AS varchar) , ' @ ') from areati.ti_wms_carga_electrolux aux
                
                        where aux.numero_guia = eltx.numero_guia) AS "Código de Producto",
                        -- CAST (eltx.numero_guia AS varchar) AS "Código de Producto",
                        --'(Electrolux) ' ||eltx.nombre_item AS "Descripción del Producto",
                
                        '(Electrolux) ' || (select string_agg(aux.nombre_item , ' - ') from areati.ti_wms_carga_electrolux aux
                
                        where aux.numero_guia = eltx.numero_guia) AS "Descripción del Producto",
                        --eltx.cantidad AS "Cantidad de Producto",
                
                        (select count(*) from areati.ti_wms_carga_electrolux eltx_a where eltx_a.numero_guia = eltx.numero_guia) as "Cantidad de Producto",
                        1 AS "Peso",
                        1 AS "Volumen",
                        1 AS "Dinero",
                        '8' AS "Duración min",
                        '09:00 - 21:00' AS "Ventana horaria 1",
                        '' AS "Ventana horaria 2",
                        coalesce((select 'Electrolux - ' || se.name from areati.subestado_entregas se where eltx.subestado=se.code),'Electrolux') AS "Notas",
                        --'RM - ' ||  initcap(eltx.comuna)  AS agrupador,
                        CASE
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(eltx.comuna)))='Region Metropolitana' 
                        THEN 'RM' || ' - ' || coalesce (tts.tamano,'?')
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(eltx.comuna)))='Valparaíso' 
                        THEN 'V - ' ||  initcap(eltx.comuna)
                        else 'S/A'
                END "Agrupador",
                        '' AS "Email de Remitentes",
                        '' AS "Eliminar Pedido Si - No - Vacío",
                        '' AS "Vehículo",
                    '' AS "Habilidades"
                from areati.ti_wms_carga_electrolux eltx
                left join public.ti_tamano_sku tts on tts.sku = cast(eltx.codigo_item as text)
                where (eltx.estado=0 or (eltx.estado=2 and eltx.subestado not in (7,10,12,19,43,44,50,51,70,80))) and eltx.estado not in (1,3)
                group by eltx.numero_guia,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20,21,22,23,24,25,26,27,1
                -------------------------------------------------------------------------------------------------------------------------------------
                -- SPORTEX
                -------------------------------------------------------------------------------------------------------------------------------------
                union all
                select      twcs.id_sportex AS "Código de Cliente",
                        initcap(twcs.cliente) AS "Nombre",
                        CASE 
                WHEN substring(initcap(replace(twcs.direccion,',','')) from '^\d') ~ '\d' then substring(initcap(replace(twcs.direccion,',','')) from '\d+[\w\s]+\d+')
                WHEN lower(twcs.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN 
                regexp_replace(regexp_replace(initcap(twcs.direccion), ',.*$', ''), '\s+(\d+\D+\d+).*$', ' \1')
                else substring(initcap(replace(twcs.direccion,',','')) from '^[^0-9]*[0-9]+')
                        END "Calle y Número",
                        --initcap(COALESCE(tcr.region, 'Region Metropolitana')) AS "Ciudad",
                        --initcap(twcs.comuna)  AS "Provincia/Estado",
                        case
                        when unaccent(lower(twcs.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twcs.comuna))
                    )
                        )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                    where unaccent(lower(twcs.comuna)) = unaccent(lower(oc2.comuna_name))
                    )

                        end as "Ciudad",

                        CASE
                        WHEN twcs.comuna='XIII - Metropolitana' THEN 'Region Metropolitana'
                        WHEN twcs.comuna='V - Valparaíso' THEN 'Valparaíso'
                        else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(twcs.comuna)))

                        END "Provincia/Estado",
                        '' AS "Latitud",
                        '' AS "Longitud",
                        coalesce(twcs.fono,'0') AS "Teléfono con código de país",
                        lower(twcs.correo) AS "Email",
                        CAST (twcs.id_sportex AS varchar) AS "Código de Pedido",
                        twcs.fecha_entrega AS "Fecha de Pedido",
                        'E' AS "Operación E/R",
                        twcs.id_sportex AS "Código de Producto",
                        '(Sportex) ' || coalesce(twcs.marca,'Sin Marca') AS "Descripción del Producto", 
                        1 AS "Cantidad de Producto",
                        1 AS "Peso",
                        1 AS "Volumen",
                        1 AS "Dinero",
                        '8' AS "Duración min",
                        '09:00 - 21:00' AS "Ventana horaria 1",
                        '' AS "Ventana horaria 2",
                        coalesce((select 'Sportex - ' || se.name from areati.subestado_entregas se where twcs.subestado=se.code),'Sportex') AS "Notas",
                        CASE
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(twcs.comuna)))='Region Metropolitana' THEN 'RM - P' 
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(twcs.comuna)))='Valparaíso' THEN 'V - ' ||  initcap(twcs.comuna)
                        else 'S/A'
                END "Agrupador",
                        '' AS "Email de Remitentes",
                        '' AS "Eliminar Pedido Si - No - Vacío",
                        '' AS "Vehículo",
                        '' AS "Habilidades"
                from areati.ti_wms_carga_sportex twcs
                left join ti_comuna_region tcr on
                    translate(lower(twcs.comuna),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
                where (twcs.estado=0 or (twcs.estado=2 and twcs.subestado not in (7,10,12,19,43,44,50,51,70,80))) and twcs.estado not in (1,3)
                -------------------------------------------------------------------------------------------------------------------------------------
                -- Easy OPL
                -------------------------------------------------------------------------------------------------------------------------------------
                union all  
                select  easygo.rut_cliente AS "Código de Cliente",
                        initcap(easygo.nombre_cliente) AS "Nombre",
                        initcap(easygo.direc_despacho) AS "Calle y Número",
                        case
                        when unaccent(lower(easygo.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easygo.comuna_despacho))
                    )
                        )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                    where unaccent(lower(easygo.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                    )

                        end as "Ciudad",

                        --initcap(COALESCE(tcr.region, 'Region Metropolitana')) AS "Ciudad",
                        --initcap(easygo.comuna_despacho)  AS "Provincia/Estado",

                        CASE
                WHEN easygo.comuna_despacho='XIII - Metropolitana' THEN 'Región Metropolitana'
                WHEN easygo.comuna_despacho='V - Valparaíso' THEN 'Valparaíso'
                else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easygo.comuna_despacho)))

                        END "Provincia/Estado",
                        '' AS "Latitud",
                        '' AS "Longitud",
                        coalesce(easygo.fono_cliente ,'0') AS "Teléfono con código de país",
                        lower(easygo.correo_cliente) AS "Email",
                        CAST (easygo.suborden AS varchar) AS "Código de Pedido",
                        easygo.fec_compromiso AS "Fecha de Pedido",
                        'E' AS "Operación E/R",
                        --easygo.id_entrega AS "Código de Producto",
                        (select string_agg(CAST(aux.codigo_sku AS varchar) , ' @ ') from areati.ti_carga_easy_go_opl aux
                
                        where aux.suborden = easygo.suborden) AS "Código de Producto",
                
                        '(Easy OPL) ' || (select string_agg(aux.descripcion , ' - ') from areati.ti_carga_easy_go_opl aux
                
                        where aux.suborden = easygo.suborden) AS "Descripción del Producto",
                
                        (select count(*) from areati.ti_carga_easy_go_opl easy_a where easy_a.suborden = easygo.suborden) AS "Cantidad de Producto",
                        1 AS "Peso",
                        1 AS "Volumen",
                        1 AS "Dinero",
                        '8' AS "Duración min",
                        '09:00 - 21:00' AS "Ventana horaria 1",
                        '' AS "Ventana horaria 2",
                        coalesce((select 'Easy OPL - ' || se.name from areati.subestado_entregas se where easygo.subestado=se.code),'Easy OPL') AS "Notas",
                        CASE
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easygo.comuna_despacho)))='Region Metropolitana' 
                        THEN 'RM' || ' - ' || coalesce (tts.tamano,'?') 
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easygo.comuna_despacho)))='Valparaíso' THEN 'V - ' ||  initcap(easygo.comuna_despacho)
                        else 'S/A'
                END "Agrupador",
                        '' AS "Email de Remitentes",
                        '' AS "Eliminar Pedido Si - No - Vacío",
                        '' AS "Vehículo",
                        '' AS "Habilidades"
                from areati.ti_carga_easy_go_opl easygo
                left join ti_comuna_region tcr on
                    translate(lower(easygo.comuna_despacho),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
                left join public.ti_tamano_sku tts on tts.sku = cast(easygo.codigo_sku as text)
                where (easygo.estado=0 or (easygo.estado=2 and easygo.subestado not in (7,10,12,19,43,44,50,51,70,80))) and easygo.estado not in (1,3)
                group by easygo.suborden,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20,21,22,23,24,25,26,27,1

            """)
            return cur.fetchall()

    @reconnect_if_closed_postgres
    def read_NS_beetrack_mensual(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            select fecha as "FECHA", id_ruta as "ID. RUTA", driver as "DRIVER", patente as "PATENTE",region as "REGION", km_ruta as "Km. Ruta",t_ped as "T-PED",
            easy as "Easy", electrolux as "Electrolux", sportex as "Sportex", imperial as "Imperial", pbb as "PBB", virutex as "Virutex", r1 as "R1", r2 as "R2",
            r3 as "R3", vr as "VR", h1100 as "C11", p10 as "(%) 11", h1300 as "C13", p40 as "(%) 13", h1500 as "C15", p60 as "(%)15",  h1700 as "C17", p80 as "(%)17",  h1800 as "C18",
            p95 as "(%)18", h2000 as "C20", p100 as "(%)20", cierre_dia as "Final_D", observ_ruta as "OBSERV-RUTA", h_inc as "H_INIC", h_term as "H_TERM", tt_ruta as "TT-RUTA", prom_ent as "Prom. ENT",
            t_ent as "T-ENT", n_ent as "N-ENT", ee as "EE", sm as "SM", ca as "CA", da as "DA", rxd as "RxD", dne as "DNE", dncc as "DNCC", d_err as "D.ERR", inc_t as "INC.T",
            dform as "DFORM", pincom as "PINCOM", speli as "SPELI", pncorr as "PNCORR", pfalt as "PFALT", pparc as "PPARC", p_dupl as "P.DUPL", r as "R", pedidos as "Pedidos"
            from areati.mae_ns_ruta_beetrack
            where to_char(fecha,'mm')=to_char(current_date,'mm')
            order by fecha asc
            """)

            return cur.fetchall()
    

     # def update_valor_rutas(self, valoresActualizados):
    #      with self.conn.cursor() as cur:
    #     #se transforma los datos obtenidos en tupla de objetos
    #         data = [(item.id_ruta, item.valor_ruta, item.id_user, item.ids_user) for item in valoresActualizados]
    #         print(data)
    #         #se debe realizar un cast debido a que el objeto que se recibe es distinto al que se requiere, se transforma
    #         #al objeto de la bd que seria una lista de objetos
    #         cur.execute("""
    #             SELECT rutas.asignar_valor_ruta(CAST(%s AS rutas.objeto_exo[]));
    #             """, (data,))
    #         self.conn.commit()
        
    @reconnect_if_closed_postgres
    def update_valor_rutas(self, valoresActualizados):
        with self.conn.cursor() as cur:
        
            data = [(item.id_ruta, item.valor_ruta, item.id_user, item.ids_user) for item in valoresActualizados]

        # Transforma los datos en una cadena de texto se le concatena ROW y el CAST 
            records_str = ','.join([f"ROW{record}::rutas.objeto_exo" for record in data])
            print(records_str)

            cur.execute(f"SELECT rutas.asignar_valor_ruta(ARRAY[{records_str}])")
            return cur.fetchone()
        #self.conn.commit()


    ## Reportes de productos entregados
    @reconnect_if_closed_postgres
    def read_reporte_producto_entregado_mensual(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            --------------------------- Reporte Productos Entregados en el Mes por Cliente
            SELECT
            case
                When (to_char(CURRENT_DATE+ i,'d') = '1') then 'Domingo'
                When (to_char(CURRENT_DATE+ i,'d') = '2') then 'Lunes'
                When (to_char(CURRENT_DATE+ i,'d') = '3') then 'Martes'
                When (to_char(CURRENT_DATE+ i,'d') = '4') then 'Miercoles'
                When (to_char(CURRENT_DATE+ i,'d') = '5') then 'Jueves'
                When (to_char(CURRENT_DATE+ i,'d') = '6') then 'Viernes'
                When (to_char(CURRENT_DATE+ i,'d') = '7') then 'Sabado'
                End "Día",
            CURRENT_DATE+ i as "Fecha",
            (
                -- ELECTROLUX
                Select count(distinct(numero_guia)) from areati.ti_wms_carga_electrolux twce
                Where to_char(twce.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Electrolux",
            (
                -- SPORTEX
                Select count(*) from areati.ti_wms_carga_sportex twcs
                Where to_char(twcs.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Sportex",
            (
                -- EASY CD
                Select count(distinct(entrega)) from areati.ti_wms_carga_easy easy
                Where to_char(easy.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Easy",
            (
                -- EASY OPL
                Select count(distinct(id_entrega)) from areati.ti_carga_easy_go_opl tcego  
                Where to_char(tcego.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Easy OPL"
            --From generate_series(date'2023-01-01'- CURRENT_DATE, 0 ) i                                   -- Resumen 2023 (Descarga)
            From generate_series(date(date_trunc('month', current_date)) - CURRENT_DATE, 0 ) i             
                    -- mes en Curso (Presentar en Pantalla con refresco)

            """)

            return cur.fetchall()

    @reconnect_if_closed_postgres
    def read_reporte_producto_entregado_hoy(self):
        with self.conn.cursor() as cur:
            cur.execute("""
           --------------------------- Reporte Productos Entregados en el Mes por Cliente
            SELECT
            case
                When (to_char(CURRENT_DATE+ i,'d') = '1') then 'Domingo'
                When (to_char(CURRENT_DATE+ i,'d') = '2') then 'Lunes'
                When (to_char(CURRENT_DATE+ i,'d') = '3') then 'Martes'
                When (to_char(CURRENT_DATE+ i,'d') = '4') then 'Miercoles'
                When (to_char(CURRENT_DATE+ i,'d') = '5') then 'Jueves'
                When (to_char(CURRENT_DATE+ i,'d') = '6') then 'Viernes'
                When (to_char(CURRENT_DATE+ i,'d') = '7') then 'Sabado'
                End "Día",
            CURRENT_DATE+ i as "Fecha",
            (
                -- ELECTROLUX
                Select count(distinct(numero_guia)) from areati.ti_wms_carga_electrolux twce
                Where to_char(twce.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Electrolux",
            (
                -- SPORTEX
                Select count(*) from areati.ti_wms_carga_sportex twcs
                Where to_char(twcs.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Sportex",
            (
                -- EASY CD
                Select count(distinct(entrega)) from areati.ti_wms_carga_easy easy
                Where to_char(easy.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Easy",
            (
                -- EASY OPL
                Select count(distinct(id_entrega)) from areati.ti_carga_easy_go_opl tcego  
                Where to_char(tcego.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Easy OPL"
            --From generate_series(date'2023-01-01'- CURRENT_DATE, 0 ) i                                   -- Resumen 2023 (Descarga)
            from generate_series(0,0) i  -- dia de hoy
            """)

            return cur.fetchall()
        
    @reconnect_if_closed_postgres
    def read_reporte_producto_entregado_anual(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            --------------------------- Reporte Productos Entregados en el Mes por Cliente
            SELECT
            case
                When (to_char(CURRENT_DATE+ i,'d') = '1') then 'Domingo'
                When (to_char(CURRENT_DATE+ i,'d') = '2') then 'Lunes'
                When (to_char(CURRENT_DATE+ i,'d') = '3') then 'Martes'
                When (to_char(CURRENT_DATE+ i,'d') = '4') then 'Miercoles'
                When (to_char(CURRENT_DATE+ i,'d') = '5') then 'Jueves'
                When (to_char(CURRENT_DATE+ i,'d') = '6') then 'Viernes'
                When (to_char(CURRENT_DATE+ i,'d') = '7') then 'Sabado'
                End "Día",
            CURRENT_DATE+ i as "Fecha",
            (
                -- ELECTROLUX
                Select count(distinct(numero_guia)) from areati.ti_wms_carga_electrolux twce
                Where to_char(twce.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Electrolux",
            (
                -- SPORTEX
                Select count(*) from areati.ti_wms_carga_sportex twcs
                Where to_char(twcs.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Sportex",
            (
                -- EASY CD
                Select count(distinct(entrega)) from areati.ti_wms_carga_easy easy
                Where to_char(easy.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Easy",
            (
                -- EASY OPL
                Select count(distinct(id_entrega)) from areati.ti_carga_easy_go_opl tcego  
                Where to_char(tcego.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Easy OPL"
            From generate_series(date'2023-01-01'- CURRENT_DATE, 0 ) i                                   -- Resumen 2023 (Descarga)
            --From generate_series(date(date_trunc('month', current_date)) - CURRENT_DATE, 0 ) i             
  
            """)

            return cur.fetchall()
    
    @reconnect_if_closed_postgres
    def read_reporte_producto_entregado_por_rango_fecha(self,inicio,termino):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            --------------------------- Reporte Productos Entregados entre fechas por Cliente
            SELECT
            case
                When (to_char(date'{termino}'+ i,'d') = '1') then 'Domingo'
                When (to_char(date'{termino}'+ i,'d') = '2') then 'Lunes'
                When (to_char(date'{termino}'+ i,'d') = '3') then 'Martes'
                When (to_char(date'{termino}'+ i,'d') = '4') then 'Miercoles'
                When (to_char(date'{termino}'+ i,'d') = '5') then 'Jueves'
                When (to_char(date'{termino}'+ i,'d') = '6') then 'Viernes'
                When (to_char(date'{termino}'+ i,'d') = '7') then 'Sabado'
                End "Día",
            date'{termino}'+ i as "Fecha",
            (
                -- ELECTROLUX
                Select count(distinct(numero_guia)) from areati.ti_wms_carga_electrolux twce
                Where to_char(twce.created_at,'yyyy-mm-dd') = to_char(date'{termino}'+ i,'yyyy-mm-dd')
            ) as "Electrolux",
            (
                -- SPORTEX
                Select count(*) from areati.ti_wms_carga_sportex twcs
                Where to_char(twcs.created_at,'yyyy-mm-dd') = to_char(date'{termino}'+ i,'yyyy-mm-dd')
            ) as "Sportex",
            (
                -- EASY CD
                Select count(distinct(entrega)) from areati.ti_wms_carga_easy easy
                Where to_char(easy.created_at,'yyyy-mm-dd') = to_char(date'{termino}'+ i,'yyyy-mm-dd')
            ) as "Easy",
            (
                -- EASY OPL
                Select count(distinct(id_entrega)) from areati.ti_carga_easy_go_opl tcego  
                Where to_char(tcego.created_at,'yyyy-mm-dd') = to_char(date'{termino}'+ i,'yyyy-mm-dd')
            ) as "Easy OPL"
            --From generate_series(date'2023-01-01'- CURRENT_DATE, 0 ) i                                   -- Resumen 2023 (Descarga)
            From generate_series( date'{inicio}'- date'{termino}', 0 ) i             
  
            """)

            return cur.fetchall()
    
    ## Reportes por hora
    @reconnect_if_closed_postgres
    def read_reportes_hora(self): 
        with self.conn.cursor() as cur:
            cur.execute("""
                -- Listar Cantidad de Entregas, No de Bultos
                ---------------------------------------------------------------------------------
                SELECT TO_CHAR(intervalo,'HH24:MI') || ' - ' || TO_CHAR(intervalo + '1 hour','HH24:MI') as "Hora",
                (
                    -- ELECTROLUX
                    Select count(distinct(numero_guia)) from areati.ti_wms_carga_electrolux twce
                    Where to_char(twce.created_at,'yyyy-mm-dd HH24') = to_char(intervalo,'yyyy-mm-dd HH24')
                ) as "Electrolux",
                (
                    -- EASY CD
                    Select count(distinct(entrega)) from areati.ti_wms_carga_easy easy
                    Where to_char(easy.created_at,'yyyy-mm-dd HH24') = to_char(intervalo,'yyyy-mm-dd HH24')
                ) as "Easy CD",
                (
                    -- EASY OPL
                    Select count(distinct(id_entrega)) from areati.ti_carga_easy_go_opl tcego  
                    Where to_char(tcego.created_at,'yyyy-mm-dd HH24') = to_char(intervalo,'yyyy-mm-dd HH24')
                ) as "Easy OPL"
                from generate_series(
                        current_date + '00:00:00'::time,
                            current_date + current_time,
                        '1 hour'::interval
                    ) AS intervalo
                union all
                select 'Total' as "Hora",
                (select count(distinct(numero_guia)) from areati.ti_wms_carga_electrolux twce3 where to_char(twce3.created_at,'yyyy-mm-dd') = to_char(current_date,'yyyy-mm-dd'))  as "Electrolux",
                (select count(distinct(entrega)) from areati.ti_wms_carga_easy twce2 where to_char(twce2.created_at,'yyyy-mm-dd') = to_char(current_date,'yyyy-mm-dd')) as "Easy CD",
                (select count(distinct(id_entrega)) from areati.ti_carga_easy_go_opl twce2 where to_char(twce2.created_at,'yyyy-mm-dd') = to_char(current_date,'yyyy-mm-dd')) as "Easy OPL"
                order by 1 desc
                ---------------------------------------------------------------------------------
            """)
            
            results = cur.fetchall()  
            return results
        
    @reconnect_if_closed_postgres
    def read_reporte_ultima_hora(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                -- Listar Cantidad de Entregas, No de Bultos
                ---------------------------------------------------------------------------------
                SELECT TO_CHAR(intervalo,'HH24:MI') || ' - ' || TO_CHAR(intervalo + '1 hour','HH24:MI') as "Hora",
                (
                    -- ELECTROLUX
                    Select count(distinct(numero_guia)) from areati.ti_wms_carga_electrolux twce
                    Where to_char(twce.created_at,'yyyy-mm-dd HH24') = to_char(intervalo,'yyyy-mm-dd HH24')
                ) as "Electrolux",
                (
                    -- SPORTEX
                    Select count(*) from areati.ti_wms_carga_sportex twcs
                    Where to_char(twcs.created_at,'yyyy-mm-dd HH24') = to_char(intervalo,'yyyy-mm-dd HH24')
                ) as "Sportex",
                (
                    -- EASY CD
                    Select count(distinct(entrega)) from areati.ti_wms_carga_easy easy
                    Where to_char(easy.created_at,'yyyy-mm-dd HH24') = to_char(intervalo,'yyyy-mm-dd HH24')
                ) as "Easy CD",
                (
                    -- EASY OPL
                    Select count(distinct(id_entrega)) from areati.ti_carga_easy_go_opl tcego  
                    Where to_char(tcego.created_at,'yyyy-mm-dd HH24') = to_char(intervalo,'yyyy-mm-dd HH24')
                ) as "Easy OPL"
                FROM generate_series(
            date_trunc('hour', current_timestamp),
            date_trunc('hour', current_timestamp) + INTERVAL '1 hour',
            '1 hour'::interval
            ) AS intervalo;
            """)

            return cur.fetchall()
    
    ##Reportes easy Region
    @reconnect_if_closed_postgres
    def read_productos_easy_region(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            select
            'Easy CD' as "Origen",
            COUNT(CASE WHEN subquery.region = 'RM' THEN 1 END) AS "R. Metropolitana",
            COUNT(CASE WHEN subquery.region = 'V' THEN 1 END) AS "V Región"
            from (
            select distinct(easy.entrega) as entrega, easy.comuna as comuna,
            CASE
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna))) = 'Region Metropolitana' THEN 'RM'
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna))) = 'Valparaíso' THEN 'V'
                else 'N/A'
            END region
            from areati.ti_wms_carga_easy easy 
            where to_char(created_at,'yyyy-mm-dd')=to_char(current_date,'yyyy-mm-dd')
            ) as subquery
            union all 
            select
            'Easy Tienda' as "Origen",
            COUNT(CASE WHEN subquery.region = 'Region Metropolitana' THEN 1 END) AS "R. Metropolitana",
            COUNT(CASE WHEN subquery.region = 'Valparaíso' THEN 1 END) AS "V Región"
            from (
            select distinct(easy.suborden) as entrega, easy.comuna_despacho as comuna,
            (select initcap(tcr.region) from public.ti_comuna_region tcr 
            where unaccent(lower(tcr.comuna))=unaccent(lower(case
            when unaccent(lower(easy.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
            (select oc.comuna_name from public.op_comunas oc 
            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
            where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna_despacho))
                        )
            )
            else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                    where unaccent(lower(easy.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                    )
            end))) as region
            --select * 
            from areati.ti_carga_easy_go_opl easy 
            where to_char(created_at,'yyyy-mm-dd')=to_char(current_date,'yyyy-mm-dd')
            ) as subquery
            """)

            return cur.fetchall()

    # Pedidos Con Fecha de Compromiso sin Despacho
    @reconnect_if_closed_postgres
    def read_pedido_compromiso_sin_despacho(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            ---------------------------------------------------------------------------------------------------------------------------
            -- (1) Corrección para ver estados y subestados para Productos con fecha de compromiso sin despacho
            ---------------------------------------------------------------------------------------------------------------------------
            select 'Easy CD' as "Origen",
            entrega as "Cod. Entrega",
            to_char(created_at,'yyyy-mm-dd') as "Fecha Ingreso",
            to_char(fecha_entrega,'yyyy-mm-dd') as "Fecha Compromiso",
            CASE
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna))) = 'Region Metropolitana' THEN 'Region Metropolitana'
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna))) = 'Valparaíso' THEN 'Valparaíso'
                else 'N/A'
            END "Region",
            initcap(easy.comuna) as "Comuna",
            easy.descripcion as "Descripcion",
            easy.bultos as "Bultos",
            (select ee.descripcion from areati.estado_entregas ee where ee.estado = easy.estado) as "Estado",
            (select se."name"  from areati.subestado_entregas se where se.code = easy.subestado) as "Subestado",
            CASE
            WHEN MAX(verified::int) = 1 THEN 1
            ELSE 0
            END AS "Verificado",
            CASE
            WHEN MAX(recepcion::int) = 1 THEN 1
            ELSE 0
            END AS "Recibido"
            from areati.ti_wms_carga_easy easy 
            where to_char(easy.fecha_entrega,'yyyymmdd') <= to_char(current_date,'yyyymmdd')
            and (easy.estado=0 or (easy.estado=2 and easy.subestado not in (7,10,12,19,43,44,50,51,70,80)))
            and easy.entrega not in (select guia from quadminds.ti_respuesta_beetrack)
            group by 1,2,3,4,5,6,7,8,9,10
            union all
            ------------------------------------------------------------------------------------------------------------------------------
            select 'Electrolux' as "Origen",
            numero_guia as "Cod. Entrega",
            to_char(created_at,'yyyy-mm-dd') as "Fecha Ingreso",
            to_char(fecha_max_entrega,'yyyy-mm-dd')  as "Fecha Compromiso",
            initcap(split_part(direccion,',',3)) AS "Region",
            initcap(split_part(direccion,',',2))  AS "Comuna",
            nombre_item as "Descripcion",
            cantidad as "Bultos",
            (select ee.descripcion from areati.estado_entregas ee where ee.estado = twce.estado) as "Estado",
            (select se."name"  from areati.subestado_entregas se where se.code = twce.subestado) as "Subestado",
            CASE
            WHEN MAX(verified::int) = 1 THEN 1
            ELSE 0
            END AS "Verificado",
            CASE
            WHEN MAX(recepcion::int) = 1 THEN 1
            ELSE 0
            END AS "Recibido"
            from areati.ti_wms_carga_electrolux twce 
            where to_char(twce.fecha_min_entrega,'yyyymmdd') <= to_char(current_date,'yyyymmdd')
            and (twce.estado=0 or (twce.estado=2 and twce.subestado not in (7,10,12,19,43,44,50,51,70,80)))
            and twce.numero_guia not in (select guia from quadminds.ti_respuesta_beetrack)
            group by 1,2,3,4,5,6,7,8,9,10
            union all
            ------------------------------------------------------------------------------------------------------------------------------
            select 'Sportex' as "Origen",
            id_sportex as "Cod. Entrega",
            to_char(created_at,'yyyy-mm-dd') as "Fecha Ingreso",
            to_char(fecha_entrega,'yyyy-mm-dd')  as "Fecha Compromiso",
            CASE
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(twcs.comuna))) = 'Region Metropolitana' THEN 'Region Metropolitana'
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(twcs.comuna))) = 'Valparaíso' THEN 'Valparaíso'
                else 'N/A'
            END "Region",
            initcap(comuna)  AS "Comuna",
            marca as "Descripcion",
            1 as "Bultos",
            (select ee.descripcion from areati.estado_entregas ee where ee.estado = twcs.estado) as "Estado",
            (select se."name"  from areati.subestado_entregas se where se.code = twcs.subestado) as "Subestado",
            CASE
            WHEN MAX(verified::int) = 1 THEN 1
            ELSE 0
            END AS "Verificado",
            CASE
            WHEN MAX(recepcion::int) = 1 THEN 1
            ELSE 0
            END AS "Recibido"
            from areati.ti_wms_carga_sportex twcs  
            where to_char(twcs.fecha_entrega ,'yyyymmdd') <= to_char(current_date,'yyyymmdd')
            and (twcs.estado=0 or (twcs.estado=2 and twcs.subestado not in (7,10,12,19,43,44,50,51,70,80)))
            and twcs.id_sportex not in (select guia from quadminds.ti_respuesta_beetrack)
            group by 1,2,3,4,5,6,7,8,9,10
            union all
            ------------------------------------------------------------------------------------------------------------------------------
            select 'Easy Tienda' as "Origen",
            suborden as "Cod. Entrega",
            to_char(created_at,'yyyy-mm-dd') as "Fecha Ingreso",
            to_char(fec_compromiso,'yyyy-mm-dd') as "Fecha Compromiso",
            CASE
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna_despacho))) = 'Region Metropolitana' THEN 'Region Metropolitana'
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna_despacho))) = 'Valparaíso' THEN 'Valparaíso'
                else 'N/A'
            END "Region",
            initcap(easy.comuna_despacho) as "Comuna",
            easy.descripcion as "Descripcion",
            easy.unidades as "Bultos",
            (select ee.descripcion from areati.estado_entregas ee where ee.estado = easy.estado) as "Estado",
            (select se."name"  from areati.subestado_entregas se where se.code = easy.subestado) as "Subestado",
            CASE
            WHEN MAX(verified::int) = 1 THEN 1
            ELSE 0
            END AS "Verificado",
            CASE
            WHEN MAX(recepcion::int) = 1 THEN 1
            ELSE 0
            END AS "Recibido"
            from areati.ti_carga_easy_go_opl easy 
            where to_char(easy.fec_compromiso,'yyyymmdd') <= to_char(current_date,'yyyymmdd')
            and (easy.estado=0 or (easy.estado=2 and easy.subestado not in (7,10,12,19,43,44,50,51,70,80)))
            and easy.suborden not in (select guia from quadminds.ti_respuesta_beetrack)
            group by 1,2,3,4,5,6,7,8,9,10
            union all
            ------------------------------------------------------------------------------------------------------------------------------
            select 'Retiro ' || rtc.cliente as "Origen", -- select * from areati.ti_retiro_cliente trc 
            rtc.cod_pedido as "Cod. Entrega",
            to_char(rtc.created_at,'yyyy-mm-dd') as "Fecha Ingreso",
            to_char(rtc.fecha_pedido,'yyyy-mm-dd') as "Fecha Compromiso",
            CASE
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(rtc.comuna))) = 'Region Metropolitana' THEN 'Region Metropolitana'
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(rtc.comuna))) = 'Valparaíso' THEN 'Valparaíso'
                else 'N/A'
            END "Region",
            initcap(rtc.comuna) as "Comuna",
            rtc.descripcion as "Descripcion",
            rtc.cantidad as "Bultos",
            (select ee.descripcion from areati.estado_entregas ee where ee.estado = rtc.estado) as "Estado",
            (select se."name"  from areati.subestado_entregas se where se.code = rtc.subestado) as "Subestado",
            CASE
            WHEN MAX(verified::int) = 1 THEN 1
            ELSE 0
            END AS "Verificado",
            CASE
            WHEN MAX(verified::int) = 1 THEN 1
            ELSE 0
            END AS "Recibido"
            from areati.ti_retiro_cliente rtc 
            where to_char(rtc.fecha_pedido,'yyyymmdd') <= to_char(current_date,'yyyymmdd')
            and (rtc.estado=0 or (rtc.estado=2 and rtc.subestado not in (7,10,12,19,43,44,50,51,70,80)))
            and rtc.cod_pedido not in (select guia from quadminds.ti_respuesta_beetrack)
            group by 1,2,3,4,5,6,7,8,9,10
                       """)

            return cur.fetchall()

    @reconnect_if_closed_postgres   
    def read_pedidos(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            select 
                (select COUNT(*) AS total_registros FROM beetrack.ruta_transyanez WHERE created_at::date = CURRENT_DATE
                        and fechaentrega <> '') as "TOTAL",
                (
                SELECT COUNT(*) AS total_registros FROM beetrack.ruta_transyanez WHERE created_at::date = CURRENT_DATE
                and lower(estado) in ('entregado','retirado') and fechaentrega <> '')as "ENTREGADO",
                (
                SELECT COUNT(*) AS total_registros FROM beetrack.ruta_transyanez WHERE created_at::date = CURRENT_DATE
                and lower(estado) not in ('entregado','retirado') and estado notnull and fechaentrega <> '') as "NO ENTREGADOS",
                (
                SELECT COUNT(*) AS total_registros FROM beetrack.ruta_transyanez
                WHERE created_at::date = CURRENT_DATE and  estado isnull
                and fechaentrega <> ''
                ) as "EN RUTA"

            """)

            return cur.fetchall()

    @reconnect_if_closed_postgres   
    def read_ruta_beetrack_hoy(self):
        with self.conn.cursor() as cur:
            cur.execute("""
           -- select 1 as numero,  ruta_beetrack,patente,driver,inicio,region,total_pedidos,h1100_10,h1300_40,h1500_60,h1700_80,h1800_95,h2000_100,entregados,no_entregados,
              --      case when estado_ruta = 'Finalizado' then 100
              --          else 0
             --       end
           -- from rutas.panel_resumen_rutas(); 
                        
                SELECT json_agg(json_build_object(
                    'Numero', 1,
                    'Ruta', r.ruta_beetrack,
                    'Patente', r.patente,
                    'Tipo', r.tipo,
                    'Driver', r.driver,
                    'Inicio', r.inicio,
                    'Region', r.region,
                    'Total_pedidos', r.total_pedidos,
                    'Once', r.h1100_10,
                    'P_once', r.p_h1100_10,
                    'Una', r.h1300_40,
                    'P_una', r.p_h1300_40,
                    'Tres', r.h1500_60,
                    'P_tres', r.p_h1500_60,
                    'Cinco', r.h1700_80,
                    'P_cinco', r.p_h1700_80,
                    'Seis', r.h1800_95,
                    'P_seis', r.p_h1800_95,
                    'Ocho', r.h2000_100,
                    'P_ocho', r.p_h2000_100,
                    'Entregados', r.entregados,
                    'No_entregados', r.no_entregados,
                    --'Porcentaje', r.rut_empresa,
                    'Porcentaje', 
                        case when r.estado_ruta = 'Finalizado' then 100
                     	   else 0
                    	end,
                   	'P_hora_actual' , 
                        case WHEN EXTRACT(HOUR FROM NOW()) < 13 THEN 10
                            WHEN EXTRACT(HOUR FROM NOW()) < 15 then 40	
                            WHEN EXTRACT(HOUR FROM NOW()) < 17 then 60
                            WHEN EXTRACT(HOUR FROM NOW()) < 18 then 80
                            WHEN EXTRACT(HOUR FROM NOW()) < 20 then 95
                            WHEN EXTRACT(HOUR FROM NOW()) < 24 then 100
                        end
	
                ))
            from rutas.panel_resumen_rutas_v2() as r;
                
                        
            """)

            return cur.fetchone()
    
    @reconnect_if_closed_postgres
    def read_pedidos_tiendas_easy_opl(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            --------------- Agrupar por O.C. ------------------------------------------------------
            select coalesce (mtet."local",'* Sin Información') as "Tienda",
            count(distinct(tcego.id_entrega)) as "Productos"
            from areati.ti_carga_easy_go_opl tcego 
            left join areati.metabase_tienda_easy_temp mtet on mtet.id_entrega = tcego.id_entrega  
            where to_char(tcego.created_at,'yyyymmdd')>=to_char(current_date,'yyyymmdd')
            group by 1
            union all 
            select 'Total' as "Tienda",
            count(distinct(id_entrega)) from areati.ti_carga_easy_go_opl tcego2 
            where to_char(tcego2.created_at,'yyyymmdd')>=to_char(current_date,'yyyymmdd')


            """)

            return cur.fetchall()
        
    @reconnect_if_closed_postgres    
    def read_pedidos_sin_tienda(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            select 
            tcego2.suborden as "Suborden",
            tcego2.id_entrega as "Id. Entrega",
            tcego2.descripcion as "Descripción",
            tcego2.unidades as "Unidades",
            tcego2.fec_compromiso as "Fec. Compromiso"
            from areati.ti_carga_easy_go_opl tcego2 
            where tcego2.id_entrega in(
            select REGEXP_REPLACE(cbase.tienda, '[^0-9]', '', 'g')
            from
            (
            select coalesce (mtet."local",'* ' || tcego.id_entrega  ) as tienda,
            count(*) as conteo
            from areati.ti_carga_easy_go_opl tcego 
            left join areati.metabase_tienda_easy_temp mtet on mtet.id_entrega = tcego.id_entrega  
            where to_char(tcego.created_at,'yyyymmdd')=to_char(current_date ,'yyyymmdd')
            group by 1
            ) cbase
            where cbase.tienda like '* %')
            """)

            return cur.fetchall()

    @reconnect_if_closed_postgres
    def get_timezone(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            SHOW timezone
            """)
            return cur.fetchall()


    ## Pedidos Pendientes En ruta 
    @reconnect_if_closed_postgres
    def read_pedidos_pendientes_total(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT
               -- COUNT(*) AS total_registros,
                COUNT(CASE WHEN fechaentrega::date < CURRENT_DATE THEN 1 END) AS atrasadas,
                COUNT(CASE WHEN fechaentrega::date = CURRENT_DATE THEN 1 END) AS en_fecha,
                COUNT(CASE WHEN fechaentrega::date > CURRENT_DATE THEN 1 END) AS adelantadas
            FROM beetrack.ruta_transyanez
            WHERE created_at::date = CURRENT_DATE
             and fechaentrega <> '';

            """)
            return cur.fetchall()
    
    @reconnect_if_closed_postgres
    def read_pedidos_pendientes_no_entregados(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT
                --COUNT(*) AS total_registros,
                COUNT(CASE WHEN fechaentrega::date < CURRENT_DATE THEN 1 END) AS atrasadas,
                COUNT(CASE WHEN fechaentrega::date = CURRENT_DATE THEN 1 END) AS en_fecha,
                COUNT(CASE WHEN fechaentrega::date > CURRENT_DATE THEN 1 END) AS adelantadas
            FROM beetrack.ruta_transyanez
            WHERE created_at::date = CURRENT_DATE
            and lower(estado) not in ('entregado','retirado') and estado notnull
            and fechaentrega <> '';

            """)
            return cur.fetchall()
        
    @reconnect_if_closed_postgres
    def read_pedidos_pendientes_en_ruta(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT
                --COUNT(*) AS total_registros,
                COUNT(CASE WHEN fechaentrega::date < CURRENT_DATE THEN 1 END) AS atrasadas,
                COUNT(CASE WHEN fechaentrega::date = CURRENT_DATE THEN 1 END) AS en_fecha,
                COUNT(CASE WHEN fechaentrega::date > CURRENT_DATE THEN 1 END) AS adelantadas
            FROM beetrack.ruta_transyanez
            WHERE created_at::date = CURRENT_DATE
            and  estado isnull and fechaentrega <> '';

            """)
            return cur.fetchall()

    @reconnect_if_closed_postgres
    def read_pedidos_pendientes_entregados(self):
        with self.conn.cursor() as cur:
            cur.execute("""
           SELECT
                --COUNT(*) AS total_registros,
                COUNT(CASE WHEN fechaentrega::date < CURRENT_DATE THEN 1 END) AS atrasadas,
                COUNT(CASE WHEN fechaentrega::date = CURRENT_DATE THEN 1 END) AS en_fecha,
                COUNT(CASE WHEN fechaentrega::date > CURRENT_DATE THEN 1 END) AS adelantadas
            FROM beetrack.ruta_transyanez
            WHERE created_at::date = CURRENT_DATE
            and lower(estado) in ('entregado','retirado')
            and fechaentrega <> '';
            """)
            return cur.fetchall()


    ### Obtener productos
    @reconnect_if_closed_postgres
    def get_producto_picking(self):

        with self.conn.cursor() as cur:
            cur.execute("""
            select * from areati.buscar_producto_picking('2905254022');
            """)
            return cur.fetchone()
        
    @reconnect_if_closed_postgres    
    def get_producto_picking_id(self, producto_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select areati.buscar_producto_picking('{producto_id}');
            """)
            return cur.fetchone()
        
    ## productos picking SKU
    @reconnect_if_closed_postgres
    def read_producto_sku(self,codigo_sku):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from areati.busca_producto_sku('{codigo_sku}')
            """)
            return cur.fetchall()

    ## Comparacion API VS WMS
    @reconnect_if_closed_postgres
    def read_carga_easy_api(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            -- API
            select nro_carga,count(distinct(entrega)) from areati.ti_wms_carga_easy
            where to_char(created_at,'yyyymmdd')>=to_char(current_date,'yyyymmdd')
            group by 1
            order by 1 asc
            """)

            return cur.fetchall()
        
    @reconnect_if_closed_postgres
    def read_carga_easy_wms(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            -- WMS
            select nro_carga,count(distinct(entrega)) from public.ti_wms_carga_easy_paso
            where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            group by 1
            order by 1 asc
            """)

            return cur.fetchall()


    ## productos sin clasificar
    @reconnect_if_closed_postgres
    def read_productos_sin_clasificar(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            ----- EASY CD (HOY)
            select DISTINCT ON (sku) sku,
            descripcion,
            talla
            from (
            select cast(easy.producto as text) as sku, 
            easy.descripcion as descripcion,
            coalesce (tts.tamano,'?') as talla
            from areati.ti_wms_carga_easy easy 
            left join public.ti_tamano_sku tts on tts.sku = cast(easy.producto as text)
            where to_char(easy.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            ) as datosbase
            where datosbase.talla ='?'
            -------------------
            union all
            ----- EASY OPL (HOY)
            select DISTINCT ON (sku) sku,
            descripcion,
            talla
            from (
            select cast(easy.codigo_sku as text) as sku, 
            easy.descripcion as descripcion,
            coalesce (tts.tamano,'?') as talla
            from areati.ti_carga_easy_go_opl easy 
            left join public.ti_tamano_sku tts on tts.sku = cast(easy.codigo_sku as text)
            where to_char(easy.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            ) as datosbase
            where datosbase.talla ='?'
            -------------------
            union all
            ----- Electrolux (HOY)
            select DISTINCT ON (sku) sku,
            descripcion,
            talla
            from (
            select cast(eltx.codigo_item as text) as sku, 
            eltx.nombre_item as descripcion,
            coalesce (tts.tamano,'?') as talla
            from areati.ti_wms_carga_electrolux eltx 
            left join public.ti_tamano_sku tts on tts.sku = cast(eltx.codigo_item as text)
            where to_char(eltx.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            ) as datosbase
            where datosbase.talla ='?'

            """)

            return cur.fetchall()
        
    @reconnect_if_closed_postgres
    def write_producto_sin_clasificar(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO public.ti_tamano_sku (sku, descripcion, tamano, origen)
            VALUES(%(SKU)s, %(Descripcion)s, %(Talla)s, %(Origen)s);
            """,data)
        self.conn.commit()

    ### insertar datos en quadmind.pedidos_planificados
    # @reconnect_if_closed_postgres
    # def write_pedidos_planificados(self, data, posicion, direccion):
    #     # print(data)
    #     with self.conn.cursor() as cur: 
    #         consulta = f"""
    #         INSERT INTO quadminds.pedidos_planificados
    #         (cod_cliente, razon_social, domicilio, tipo_cliente, fecha_reparto, cod_reparto, maquina, chofer, fecha_pedido, 
    #         cod_pedido, cod_producto, producto, cantidad, horario, arribo, partida, peso, volumen, dinero, posicion)
    #         values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    #         """
    #         # Ejecutar la consulta con los parámetros
    #         cur.execute(consulta, (
    #             data['Código cliente'], data['Razón social'], direccion,
    #             data['Tipo de Cliente'], data['Fecha de Reparto'], data['Codigo Reparto'],
    #             data['Máquina'], data['Chofer'], data['Fecha De Pedido'], data['Codigo de Pedido'],
    #             data['Codigo de Producto'], data['Producto'], data['Cantidad'], data['Ventana Horaria'],
    #             data['Arribo'], data['Partida'], data['Peso (kg)'], data['Volumen (m3)'],
    #             data['Dinero ($)'], posicion
    #         ))

    #     self.conn.commit()

    
    ### Insertar datos en tabla quadmind.ruta_manual
    @reconnect_if_closed_postgres
    def get_ruta_tracking_producto(self,pedido_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            --SELECT * FROM quadminds.recupera_datos_pedido('{pedido_id}');

            SELECT
                "Código de Pedido",
                "Fecha de Pedido",
                "Fecha Original Pedido",
                "Código de Producto",
                "Descripción del Producto",
                "Cantidad de Producto",
                "Notas",
                "Cod. SKU",
                "Pistoleado",
                "Código de Cliente",
                "Nombre",
                "Calle y Número",
                "Dirección Textual",
                "Ciudad",
                "Teléfono con código de país",
                "Email",
                COALESCE(string_agg(DISTINCT drm.nombre_ruta::varchar, '@'), 'Sin Ruta Asignada') AS "Ruta Hela"
            FROM
                areati.busca_ruta_manual_base2('{pedido_id}')
            LEFT JOIN
                quadminds.datos_ruta_manual drm ON drm.cod_pedido = '{pedido_id}' AND drm.cod_producto = "Código de Producto"
            GROUP BY
                "Código de Pedido", "Fecha de Pedido", "Fecha Original Pedido",
                "Código de Producto", "Descripción del Producto",
                "Cantidad de Producto", "Notas", "Cod. SKU", "Pistoleado",
                "Código de Cliente","Nombre","Calle y Número","Dirección Textual",
                "Ciudad","Teléfono con código de país","Email";

            """)
            return cur.fetchall()

    @reconnect_if_closed_postgres
    def get_ruta_manual(self,pedido_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            ---select * from areati.busca_ruta_manual_base2('{pedido_id}')

            select "Código de Cliente","Nombre","Calle y Número","Dirección Textual",
                    "Ciudad","Provincia/Estado","Latitud","Longitud","Teléfono con código de país",
                    "Email","Código de Pedido","Fecha de Pedido","Fecha Original Pedido","Operación E/R",
                    "Código de Producto","Descripción del Producto","Cantidad de Producto","Peso",
                    "Volumen","Dinero","Duración min","Ventana horaria 1","Ventana horaria 2","Notas",
                    "Agrupador","Email de Remitentes","Eliminar Pedido Si - No - Vacío","Vehículo",
                    "Habilidades","Cod. SKU","Pistoleado","Talla", "Estado Entrega","En Ruta",
                    "TOC","Observacion TOC","Sistema","Obs. Sistema"
            FROM areati.busca_ruta_manual_base2('{pedido_id}');
            """)
            return cur.fetchall()
        
    ## buscar ruta
    @reconnect_if_closed_postgres
    def get_datos_producto_en_ruta(self,pedido_id):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select distinct on (ruta."Código de Producto", "Cod. SKU" ) ruta."Código de Pedido", ruta."Código de Producto", ruta."Descripción del Producto", ruta."Ciudad",ruta."Provincia/Estado",ruta."Fecha de Pedido",ruta."Fecha Original Pedido"
            ,coalesce (drm.nombre_ruta , 'No asignada'), ruta."Notas", ruta."Cantidad de Producto"
            from areati.busca_ruta_manual_base2('{pedido_id}') ruta
            left join quadminds.datos_ruta_manual drm on drm.cod_pedido = ruta."Código de Pedido" and drm.estado = true

                        """)
            return cur.fetchall()

    @reconnect_if_closed_postgres
    def get_rutas_activas(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select subquery.nombre_ruta from (
            SELECT nombre_ruta,
                CASE WHEN bool_or(estado = FALSE) THEN FALSE ELSE TRUE END AS estado
            FROM quadminds.datos_ruta_manual
            GROUP BY nombre_ruta) subquery
            where subquery.estado = true


            """)

            return cur.fetchall()

    
    @reconnect_if_closed_postgres        
    def get_factura_electrolux(self,pedido_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select codigo_item , folio_factura from areati.ti_wms_carga_electrolux eltx 
            where eltx.numero_guia = '{pedido_id}' or 
            trim(eltx.factura) = trim('{pedido_id}') or 
            trim(eltx.folio_factura) = trim('{pedido_id}') 
            """)
            return cur.fetchall()

    @reconnect_if_closed_postgres
    def get_numero_guia_by_factura(self,factura):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select numero_guia from areati.ti_wms_carga_electrolux eltx 
            where trim(eltx.factura) = trim('{factura}') 
            or trim(eltx.folio_factura) = trim('{factura}')
            limit 1
            """)
            return cur.fetchone()
    
    @reconnect_if_closed_postgres
    def direccion_textual(self,pedido_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select "Dirección Textual" from areati.busca_ruta_manual_base2('{pedido_id}')
            """)
            return cur.fetchall()
    
    @reconnect_if_closed_postgres
    def get_cod_producto_ruta_manual(self,pedido_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select "Código de Producto", "Descripción del Producto"  from areati.busca_ruta_manual_base2('{pedido_id}')
            """)
            return cur.fetchall()
        
    @reconnect_if_closed_postgres
    def get_comuna_por_ruta_manual(self,pedido_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select "Ciudad" , "Provincia/Estado"  from areati.busca_ruta_manual_base2('{pedido_id}') limit 1
            """)
            return cur.fetchone()

    @reconnect_if_closed_postgres
    def get_nombre_ruta_manual(self,created_by):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT
            TO_CHAR(coalesce (max(id_ruta)+1,1)::integer, 'FM00000')
            || '-' ||
            to_char({created_by}::integer, 'FM0000') || '-' ||
            TO_CHAR(current_date, 'YYYYMMDD') AS numero_unico
            FROM
            quadminds.datos_ruta_manual drm

            """)
            return cur.fetchall()
        
    @reconnect_if_closed_postgres
    def update_id_ruta_rutas_manuales(self ,id_ruta ,nombre_ruta ,cod_pedido):
        with self.conn.cursor() as cur:
            cur.execute(f"""        
            update quadminds.datos_ruta_manual 
            set id_ruta = {id_ruta}, agrupador = '{nombre_ruta}', nombre_ruta = '{nombre_ruta}'
            where cod_pedido = '{cod_pedido}'  
            """)
            row = cur.rowcount
        self.conn.commit()
        return row
        
    def check_producto_existe(self,codigo_pedido):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             select areati.check_producto('{codigo_pedido}');
            """)
            return cur.fetchone()
    
    def check_fecha_ruta_producto_existe(self,codigo_pedido):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select fecha_ruta from quadminds.datos_ruta_manual drm 
            where cod_pedido = '{codigo_pedido}' or cod_producto = '{codigo_pedido}'
            order by created_at  desc
            limit 1
            """)
            return cur.fetchone()
        
    def check_producto_codigo_repetido(self,nombre_ruta : str, cod_pedido : str, cod_producto : str, sku : str) :
        with self.conn.cursor() as cur:
            cur.execute(f"""
             select * from quadminds.datos_ruta_manual drm 
            where nombre_ruta = '{nombre_ruta}' 
            and cod_pedido = '{cod_pedido}' and cod_producto = '{cod_producto}'
            and sku = '{sku}'
            """)
            return cur.fetchone()
        
    def read_id_ruta(self):
        with self.conn.cursor() as cur:
            cur.execute("""
             select coalesce (max(id_ruta)+1,1) from quadminds.datos_ruta_manual drm
            """)

            return cur.fetchone()
        
    def write_rutas_manual(self, data):
        # print(data)
        with self.conn.cursor() as cur: 
            cur.execute("""
            INSERT INTO quadminds.datos_ruta_manual (id_ruta, fecha_ruta, nombre_ruta, cod_cliente, nombre, calle_numero, ciudad, provincia_estado, latitud, longitud, telefono, email, cod_pedido, fecha_pedido, operacion, cod_producto, desc_producto, cant_producto, peso, volumen, dinero, duracion, vent_horaria_1, vent_horaria_2, notas, agrupador, email_rem, eliminar_pedido, vehiculo, habilidades, sku, talla, estado, created_by, posicion, de, dp, pickeado)
            VALUES (%(Id_ruta)s, %(Fecha_ruta)s, %(Nombre_ruta)s, %(Codigo_cliente)s, %(Nombre)s, %(Calle)s, %(Ciudad)s,
              %(Provincia)s, %(Latitud)s, %(Longitud)s, %(Telefono)s, %(Email)s, %(Codigo_pedido)s, %(Fecha_pedido)s,
              %(Operacion)s, %(Codigo_producto)s, %(Descripcion_producto)s, %(Cantidad_producto)s, %(Peso)s, 
              %(Volumen)s, %(Dinero)s, %(Duracion_min)s, %(Ventana_horaria_1)s, %(Ventana_horaria_2)s, %(Notas)s, 
              %(Nombre_ruta)s, %(Email_remitentes)s, %(Eliminar_pedido)s, %(Vehiculo)s, %(Habilidades)s, %(SKU)s,
              %(Tamaño)s, true , %(Created_by)s, %(Posicion)s,%(DE)s, %(DP)s,%(Pistoleado)s);
            """,data)
        self.conn.commit()


    def get_fecha_ruta(self,id_ruta : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             SELECT distinct(fecha_ruta)
            FROM quadminds.datos_ruta_manual
            where id_ruta = {id_ruta}
            """)

            return cur.fetchone()

    def update_verified(self, codigo_producto):
        sql_queries = [
            "UPDATE areati.ti_wms_carga_sportex SET verified = true WHERE upper(areati.ti_wms_carga_sportex.id_sportex) = %s",
            "UPDATE areati.ti_wms_carga_easy SET verified = true WHERE areati.ti_wms_carga_easy.carton = %s",
            "UPDATE areati.ti_wms_carga_electrolux SET verified = true WHERE areati.ti_wms_carga_electrolux.numero_guia = %s",
            "UPDATE areati.ti_carga_easy_go_opl SET verified = true WHERE areati.ti_carga_easy_go_opl.id_entrega = %s",
            "UPDATE areati.ti_retiro_cliente SET verified = true WHERE areati.ti_retiro_cliente.cod_pedido  = %s"
        ]

        updates = []
        try:
            for query in sql_queries:
                with self.conn.cursor() as cur:
                    cur.execute(query, (codigo_producto,))
                    # print(cur.rowcount)
                    updates.append(cur.rowcount)
                self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error durante la actualización:", error)
        finally:
            # pass
            return updates
        

    ## obtener  cantidad de productos de rutas_en_activo
    def read_cant_productos_ruta_activa(self,nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            with data_ruta_manual as (
            SELECT 
                subquery.cod_pedido as cod_pedido,
                (areati.busca_ruta_manual_base2(subquery.cod_pedido))."Cod. SKU" as SKU,
                (areati.busca_ruta_manual_base2(subquery.cod_pedido))."Cantidad de Producto" as bultos
            FROM (
            SELECT DISTINCT ON (drm.cod_pedido) drm.cod_pedido cod_pedido
            FROM quadminds.datos_ruta_manual drm
            WHERE drm.nombre_ruta = '{nombre_ruta}'
            ) AS subquery
        )

        select * from data_ruta_manual

            """)
            return cur.fetchall()

    ## Rutas en activo

    def read_rutas_en_activo(self,nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select      drm."N°",
                        drm."Pedido",
                        drm."Comuna",
                        drm."Nombre",
                        drm."Dirección",
                        drm."teléfono",
                        rsb.sku as "SKU",
                        rsb.descripcion as "Producto",
                        rsb.cant_producto as "UND",
                        rsb.bultos as "Bult",
                        drm."DE",
                        drm."DP",
                        drm."Fecha Pedido"
            from (select 	 distinct on (drm.cod_pedido) 
                        drm.posicion as "N°",
                        drm.cod_pedido as "Pedido",
                        drm.ciudad as "Comuna",
                        drm.nombre as "Nombre",
                        drm.calle_numero "Dirección",
                        drm.telefono as "teléfono",
                        CASE
                                WHEN bool_or(drm.de) THEN 'Embalaje con Daño'
                                ELSE ''
                            END AS "DE",
                            CASE
                                WHEN bool_or(drm.dp) THEN 'Producto con Daño'
                                ELSE ''
                            END as "DP",
                        drm.fecha_pedido as "Fecha Pedido"
                        from quadminds.datos_ruta_manual drm 
                        where drm.nombre_ruta =  '{nombre_ruta}'
                        group by 1,2,3,4,5,6,9
                        order by drm.cod_pedido ) as drm  
                        LEFT JOIN LATERAL areati.recupera_sku_bultos(drm."Pedido",'{nombre_ruta}') AS rsb ON true
                        order by  drm."N°" asc
            """)
            return cur.fetchall()
        

    def read_rutas_en_activo_para_armar_excel(self,nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from rutas.recupera_datos_ruta('{nombre_ruta}');
            """)
            return cur.fetchall()
        

    def prueba_recupera_bulto_sku(self,cod_pedido,nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from areati.recupera_sku_bultos('{cod_pedido}', '{nombre_ruta}')
            """)
            return cur.fetchone()
        
    
        

    
        

    def get_fecha_asignacion_ruta(self,nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select coalesce (to_char(max(created_at),'dd-mm-yyyy hh24:mi:ss'),'') 
            from hela.ruta_asignada ra 
            where ra.nombre_ruta = '{nombre_ruta}'
            limit 1
            """)
            return cur.fetchone() 
        
    def verificar_pistoledos_en_ruta(self,cod_pedido):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select easy.verified as "Pistoleado" from areati.ti_wms_carga_easy easy where easy.entrega = '{cod_pedido}' and easy.verified = false
            union all 
            select eltx.verified as "Pistoleado" from areati.ti_wms_carga_electrolux eltx where eltx.numero_guia = '{cod_pedido}' and eltx.verified = false
            union all 
            select twcs.verified as "Pistoleado" from areati.ti_wms_carga_sportex twcs where twcs.id_sportex = '{cod_pedido}' and twcs.verified = false
            union all 
            select easygo.verified as "Pistoleado" from areati.ti_carga_easy_go_opl easygo where easygo.suborden = '{cod_pedido}' and easygo.verified = false
            union all
            select retc.verified as "Pistoleado"  from areati.ti_retiro_cliente retc where retc.cod_pedido = '{cod_pedido}' and retc.verified = false  
            limit 1  
             """)
            return cur.fetchall()


    def read_nombres_rutas(self,fecha):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT nombre_ruta,
                CASE WHEN bool_or(estado = FALSE) THEN FALSE ELSE TRUE END AS estado,
                CASE WHEN bool_or(pickeado = FALSE) THEN FALSE ELSE TRUE END AS pickeado,
                CASE WHEN bool_or(alerta = TRUE) THEN TRUE ELSE FALSE END AS alerta
            FROM quadminds.datos_ruta_manual
            WHERE TO_CHAR(fecha_ruta, 'YYYY-MM-DD') = '{fecha}'
            GROUP BY nombre_ruta;


            """)

            return cur.fetchall()
        
    ## solo las comunas de la ruta según fecha

    def read_comunas_regiones_ruta(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT DISTINCT INITCAP(LOWER(drm.ciudad)) , opr.region_name  AS ciudad
                FROM quadminds.datos_ruta_manual drm
                inner JOIN public.op_comunas oc ON INITCAP(LOWER(drm.ciudad)) = INITCAP(LOWER(oc.comuna_name))
                inner JOIN public.op_regiones opr ON oc.id_region = opr.id_region
                ORDER BY ciudad;
                        """)
            return cur.fetchall()

    def read_comunas_ruta_by_fecha(self,fecha):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select distinct(initcap(lower(ciudad))) as ciudad
                --from quadminds.datos_ruta_manual drm where TO_CHAR(fecha_ruta, 'YYYY-MM-DD') = '{fecha}'
                from quadminds.datos_ruta_manual drm
                order by ciudad 
                        """)
            return cur.fetchall()
        
    def filter_nombres_rutas_by_region(self,fecha,comuna,region):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT nombre_ruta,
                CASE WHEN bool_or(estado = FALSE) THEN FALSE ELSE TRUE END AS estado,
                CASE WHEN bool_or(pickeado = FALSE) THEN FALSE ELSE TRUE END AS pickeado,
                CASE WHEN bool_or(alerta = TRUE) THEN TRUE ELSE FALSE END AS alerta
            FROM quadminds.datos_ruta_manual
            WHERE TO_CHAR(fecha_ruta, 'YYYY-MM-DD') = '{fecha}' 
            and lower(provincia_estado) = lower('{region}') 
            GROUP BY nombre_ruta;
                              
           """)
            return cur.fetchall()
        
    def filter_nombres_rutas_by_comuna(self,fecha,comuna,region):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT nombre_ruta,
                CASE WHEN bool_or(estado = FALSE) THEN FALSE ELSE TRUE END AS estado,
                CASE WHEN bool_or(pickeado = FALSE) THEN FALSE ELSE TRUE END AS pickeado,
                CASE WHEN bool_or(alerta = TRUE) THEN TRUE ELSE FALSE END AS alerta
            FROM quadminds.datos_ruta_manual
            WHERE TO_CHAR(fecha_ruta, 'YYYY-MM-DD') = '{fecha}' 
            and lower(ciudad) = lower('{comuna}') 
            GROUP BY nombre_ruta;
                              
           """)
            return cur.fetchall()  
        
    def update_estado_rutas(self,nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""        
            update quadminds.datos_ruta_manual
            set estado = false
            where quadminds.datos_ruta_manual.nombre_ruta  = '{nombre_ruta}'
            """)
        self.conn.commit()

    ## actualizar estado de rutas para que esten abiertas

    def update_estado_rutas_a_true_abierta(self,nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""        
            update quadminds.datos_ruta_manual
            set estado = true
            where quadminds.datos_ruta_manual.nombre_ruta  = '{nombre_ruta}'
            """)
        self.conn.commit()

    def read_ruta_activa_by_nombre_ruta(self,nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
               -- select * from rutas.listar_ruta_edicion('{nombre_ruta}');

               -- select id_ruta,nombre_ruta,cod_cliente,nombre,calle_numero,ciudad,
               -- provincia_estado,telefono,email,cod_pedido,fecha_pedido,cod_producto,
               -- desc_producto,cant_producto,notas,agrupador,sku,talla,estado,posicion,
               -- fecha_ruta,de,dp,"alerta TOC","Obs. TOC",concatenated_data,alerta
               -- from rutas.listar_ruta_edicion('{nombre_ruta}');

               with data_ruta_manual as (
                SELECT distinct on (subquery.cod_pedido)
                    subquery.cod_pedido as cod_pedido,
                    (areati.busca_ruta_manual_base2(subquery.cod_pedido))."Sistema" as sistema,
                    (areati.busca_ruta_manual_base2(subquery.cod_pedido))."Obs. Sistema" as obs_sistema,
                    (areati.busca_ruta_manual_base2(subquery.cod_pedido))."Pistoleado" as pistoleado,
                    (areati.busca_ruta_manual_base2(subquery.cod_pedido))."En Ruta" as en_ruta,
                    (areati.busca_ruta_manual_base2(subquery.cod_pedido))."Estado Entrega" as estado_entrega,
                    (areati.busca_ruta_manual_base2(subquery.cod_pedido))."Fecha Original Pedido" as fecha_original
                FROM (
                SELECT DISTINCT ON (drm.cod_pedido) drm.cod_pedido cod_pedido
                FROM quadminds.datos_ruta_manual drm
                WHERE drm.nombre_ruta = '{nombre_ruta}'
                ) AS subquery
            )
            select 	 drm.id_ruta,
                    drm.nombre_ruta,
                    drm.cod_cliente,
                    drm.nombre,
                    initcap(drm.calle_numero),
                    drm.ciudad,
                    drm.provincia_estado,
                    drm.telefono,
                    drm.email,
                    drm.cod_pedido,
                    drm.fecha_pedido,
                    drm.cod_producto,
                    drm.desc_producto,
                    drm.cant_producto,
                    drm.notas,
                    drm.agrupador,
                    drm.sku,
                    drm.talla,
                    drm.estado,
                    drm.posicion,
                    drm.fecha_ruta,
                    drm.de,
                    drm.dp,
                    tbm.alerta as "alerta TOC",
                    tbm.observacion as "Obs. TOC",
                    CONCAT(
                            CASE WHEN r.sistema THEN '1' ELSE '0' END, '@',
                            COALESCE(r.obs_sistema, 'null'), '@',
                            CASE WHEN r.pistoleado THEN '1' ELSE '0' END, '@',
                            CASE WHEN r.en_ruta THEN '1' ELSE '0' END, '@',
                            r.estado_entrega
                        ) AS concatenated_data,
                    drm.alerta,
                    r.fecha_original,
                    drm.operacion,
                    drm.created_by
            FROM quadminds.datos_ruta_manual drm
            LEFT join rutas.toc_bitacora_mae tbm ON tbm.guia = drm.cod_pedido and tbm.alerta=true
            LEFT join data_ruta_manual r on r.cod_pedido = drm.cod_pedido
            WHERE drm.nombre_ruta = '{nombre_ruta}'
            ORDER BY drm.posicion;
            """)
            return cur.fetchall()

        
    def asignar_ruta_quadmind_manual(self, id , fecha):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from rutas.carga_rutas_bloque_excel({id}, '{fecha}');
            """)
            return cur.fetchall()
    
    def calcular_diferencia_tiempo(self, fecha_hoy):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT
            -- EXTRACT(HOUR FROM hora2 - hora1) || ':' ||
            EXTRACT(MINUTE FROM hora2 - hora1) || ':' ||
            EXTRACT(SECOND FROM hora2 - hora1)::integer AS diferencia
            FROM
            (
            SELECT
                MIN(created_at::TIME) as hora1,
                MAX(created_at::TIME) as hora2
            FROM
                quadminds.pedidos_planificados
            WHERE
                to_char(created_at, 'yyyymmdd') = '{fecha_hoy}'
            ) AS t;
            """)
            return cur.fetchall()
        

    def delete_ruta_antigua(self, nombre_ruta): 
        with self.conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM quadminds.datos_ruta_manual
                WHERE nombre_ruta = '{nombre_ruta}' 
                        """)
            rows_delete = cur.rowcount
        self.conn.commit() 
        return rows_delete
    
    def update_posicion(self, posicion : int, cod_pedido : str, cod_producto : str, fecha_ruta : str, de : bool , dp : bool , nombre_ruta :str, pickeado : bool): 
        with self.conn.cursor() as cur:
            cur.execute(f"""
                UPDATE quadminds.datos_ruta_manual  
                SET posicion  = {posicion}, fecha_ruta = '{fecha_ruta}', de = {de}, dp = {dp}, pickeado = {pickeado}
                WHERE quadminds.datos_ruta_manual.cod_pedido = '{cod_pedido}' AND quadminds.datos_ruta_manual.cod_producto = '{cod_producto}' and quadminds.datos_ruta_manual.nombre_ruta  = '{nombre_ruta}'
                           """)
            rows_delete = cur.rowcount
        self.conn.commit() 
        return rows_delete

    
    def delete_producto_ruta_activa(self,cod_producto, nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM quadminds.datos_ruta_manual
                WHERE cod_producto = '{cod_producto}' and nombre_ruta = '{nombre_ruta}'
                        """)
            rows_delete = cur.rowcount
        self.conn.commit() 
        return rows_delete

    def delete_productos_ruta(self,array : str,nombre_ruta : str):
        codigos = array.split(',')
        valores = array 
        with self.conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM quadminds.datos_ruta_manual
                WHERE nombre_ruta = '{nombre_ruta}' and cod_pedido in ({','.join(['%s']*len(codigos))})
                """, codigos)
            rows_delete = cur.rowcount
        self.conn.commit() 
        return rows_delete
           
        #### Recepcion Easy OPL

    def update_producto_picking_OPL(self,cod_producto,cod_sku):
        with self.conn.cursor() as cur:
            cur.execute(f"""        
            update areati.ti_carga_easy_go_opl
            set verified = true
            where areati.ti_carga_easy_go_opl.id_entrega  = '{cod_producto}' and areati.ti_carga_easy_go_opl.codigo_sku = '{cod_sku}'
            """)
        self.conn.commit()

    def read_productos_picking_OPL(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             select  easygo.rut_cliente AS "Rut de Cliente",
             initcap(easygo.nombre_cliente) AS "Nombre",
             initcap(easygo.direc_despacho) AS "Calle y Número",
             initcap(easygo.comuna_despacho)  AS "Provincia/Estado",
             --coalesce(easygo.fono_cliente ,'0') AS "Teléfono",
             CAST (easygo.suborden AS varchar) AS "Código de Pedido",
             easygo.fec_compromiso AS "Fecha de Pedido",
             easygo.id_entrega AS "Código de Producto",
             easygo.descripcion AS "Descripción del Producto",
             cast(easygo.unidades as numeric) AS "Cantidad de Producto",
             easygo.codigo_sku as "Cod. SKU",                         
             easygo.verified as "Pistoleado"   
        
            from areati.ti_carga_easy_go_opl easygo
            where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')           
                        """)           
            return cur.fetchall()
    
    def read_productos_picking_OPL_sku(self, codigo_sku):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             select  easygo.rut_cliente AS "Rut de Cliente",
             initcap(easygo.nombre_cliente) AS "Nombre",
             initcap(easygo.direc_despacho) AS "Calle y Número",
             initcap(easygo.comuna_despacho)  AS "Provincia/Estado",
             --coalesce(easygo.fono_cliente ,'0') AS "Teléfono",
             CAST (easygo.suborden AS varchar) AS "Código de Pedido",
             easygo.fec_compromiso AS "Fecha de Pedido",
             easygo.id_entrega AS "Código de Producto",
             easygo.descripcion AS "Descripción del Producto",
             cast(easygo.unidades as numeric) AS "Cantidad de Producto",
             easygo.codigo_sku as "Cod. SKU",                         
             easygo.verified as "Pistoleado"   
        
            from areati.ti_carga_easy_go_opl easygo
            where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') and easygo.codigo_sku = '{codigo_sku}'         
                        """)      
                 
            return cur.fetchall()

    ## Asignar rutas activas
    def insert_ruta_asignada(self,data):
        with self.conn.cursor() as cur: 
            cur.execute("""
            INSERT INTO hela.ruta_asignada
            (asigned_by, id_ruta, nombre_ruta, patente, driver, cant_producto, estado, region)
            VALUES(%(asigned_by)s, %(id_ruta)s, %(nombre_ruta)s, %(patente)s, %(conductor)s, %(cantidad_producto)s, true, %(region)s);
            """,data)
        self.conn.commit()

    def get_id_ruta_activa_by_nombre(self, nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select id_ruta
            from quadminds.datos_ruta_manual drm where nombre_ruta = '{nombre_ruta}'
            """)
            return cur.fetchone()
        

    def eliminar_ruta(self,nombre_ruta):
        with self.conn.cursor() as cur: 
            cur.execute(f"""
            DELETE FROM quadminds.datos_ruta_manual
            WHERE nombre_ruta = '{nombre_ruta}'
                    """)
            rows_delete = cur.rowcount
        self.conn.commit() 
        
        return rows_delete
        
    
        
    # recepcion tiendas

    # CTN000026344881
    def read_recepcion_electrolux(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
        select eltx.identificador_contacto AS "Código de Cliente",
        initcap(eltx.nombre_contacto) AS "Nombre",
        CASE 
        WHEN substring(initcap(split_part(eltx.direccion,',',1)) from '^\d') ~ '\d' then substring(initcap(split_part(eltx.direccion,',',1)) from '\d+[\w\s]+\d+')
        WHEN lower(split_part(eltx.direccion,',',1)) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN 
        regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '')
        else coalesce(substring(initcap(split_part(eltx.direccion,',',1)) from '^[^0-9]*[0-9]+'),eltx.direccion)
                END "Calle y Número",
                --initcap(split_part(eltx.direccion,',',2)) AS "Ciudad",
                --initcap(split_part(eltx.direccion,',',3)) AS "Provincia/Estado",
                case
                when unaccent(lower(eltx.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                (select oc.comuna_name from public.op_comunas oc 
                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(eltx.comuna))
            )
                )
                else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
            where unaccent(lower(eltx.comuna)) = unaccent(lower(oc2.comuna_name))
            )
        end as "Ciudad",
                CAST (eltx.numero_guia AS varchar) AS "Código de Pedido",
                eltx.fecha_min_entrega AS "Fecha de Pedido",
                CAST (eltx.codigo_item AS varchar) AS "Código de Producto",
                TRIM(eltx.nombre_item) AS "Descripción del Producto",
                cast(eltx.cantidad as numeric) AS "Cantidad de Producto",
            cast(eltx.codigo_item as text) as "Cod. SKU",					
            eltx.verified as "Pistoleado",
            eltx.recepcion as "Recepcion"
        from areati.ti_wms_carga_electrolux eltx
        -- from areati.ti_wms_carga_electrolux_hoy as eltx 
        where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
        -- order by created_at desc
        order by eltx.numero_guia 
                        """)           
            return cur.fetchall()

    def read_recepcion_easy_opl(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             select  easygo.rut_cliente AS "Rut de Cliente",
             initcap(easygo.nombre_cliente) AS "Nombre",
             initcap(easygo.direc_despacho) AS "Calle y Número",
             initcap(easygo.comuna_despacho)  AS "Provincia/Estado",
             --coalesce(easygo.fono_cliente ,'0') AS "Teléfono",
             CAST (easygo.suborden AS varchar) AS "Código de Pedido",
             easygo.fec_compromiso AS "Fecha de Pedido",
             easygo.id_entrega AS "Código de Producto",
             easygo.descripcion AS "Descripción del Producto",
             cast(easygo.unidades as numeric) AS "Cantidad de Producto",
             easygo.codigo_sku as "Cod. SKU",                         
             easygo.verified as "Pistoleado",
             easygo.id_ruta as "N carga",
             easygo.recepcion as "Recepcion",
            'comuna' as "Comuna"
        
            from areati.ti_carga_easy_go_opl easygo
            where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') 
            -- order by created_at desc  
            order by easygo.suborden      
                        """)           
            return cur.fetchall()
        
    def read_recepcion_easy_opl_detalles(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select 	subquery.id_ruta,
                subquery.suborden,
                subquery.nombre_cliente,
                subquery.comuna_despacho,
                subquery.direc_despacho,
                subquery.total_unidades,
                subquery.verificado,
                coalesce(subquery.bultos, 1)
                
                from (
                    select 	distinct on (tcego.suborden)
                            tcego.id_ruta,
                            tcego.suborden,
                            initcap(tcego.nombre_cliente) as nombre_cliente,
                            tcego.comuna_despacho,
                            tcego.direc_despacho,
                            sum(tcego.unidades) as total_unidades,
                            CASE WHEN bool_or(tcego.verified = FALSE) THEN FALSE ELSE TRUE END AS verificado,
                            tcegob.bultos  as bultos
                    from areati.ti_carga_easy_go_opl tcego  
                    left join areati.ti_carga_easy_go_opl_bultos tcegob on tcego.suborden = tcegob.suborden
                    where tcego.created_at::date = current_date 
                    group by 1,2,3,4,5,8
                ) subquery
                                """)           
            return cur.fetchall()
        

    def get_bultos_easy_opl(self,suborden):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select coalesce(bultos,1) as bultos from areati.ti_carga_easy_go_opl_bultos tcegob
                where suborden = '{suborden}'     
                        """)           
            return cur.fetchone()

    ## adicion easy OPL

    def insert_bultos_a_easy_opl(self,data):
        with self.conn.cursor() as cur: 
            cur.execute("""          
            INSERT INTO areati.ti_carga_easy_go_opl_bultos
            (id_ruta, suborden, bultos, tienda)
            VALUES(%(Id_ruta)s, %(Suborden)s, %(Bultos)s, %(Tienda)s);
            """,data)
        self.conn.commit()

    def update_bultos_a_easy_opl(self,data):
        with self.conn.cursor() as cur: 
            cur.execute("""          
            UPDATE areati.ti_carga_easy_go_opl_bultos
            SET  bultos=%(Bultos)s
            --WHERE id_ruta=%(Id_ruta)s AND suborden=%(Suborden)s;
            WHERE suborden=%(Suborden)s;
            """,data)
        self.conn.commit()

    def checK_bulto_easy_opl_si_existe(self, suborden):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from areati.ti_carga_easy_go_opl_bultos tcegob where suborden = '{suborden}'
                                """)           
            return cur.fetchall()

    def read_recepcion_sportex(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select twcs.id_sportex AS "Código de Cliente",
                initcap(twcs.cliente) as "Nombre",
                twcs.direccion as "Calle",
                twcs.comuna as "Provincia",
                CAST (twcs.id_sportex AS varchar) AS "Código de Pedido",
                twcs.fecha_entrega AS "Fecha de Pedido",
                twcs.id_sportex as "Codigo producto",
                coalesce(twcs.marca,'Sin Marca') AS "Descripción del Producto",
                1 AS "Cantidad de Producto",
                '' as "SKU",
                twcs.verified as "Pistoleado",
                twcs.recepcion as "Recepcion"
                from areati.ti_wms_carga_sportex twcs 
                where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') 
                --order by created_at desc
                order by twcs.id_sportex
                        """)           
            return cur.fetchall()
    
    def read_recepcion_easy_cd(self,dia_anterior):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select  CAST(easy.entrega AS varchar) AS "Código de Cliente",     
                    initcap(easy.nombre) AS "Nombre",
                    CASE 
                        WHEN substring(easy.direccion from '^\d') ~ '\d' then substring(initcap(easy.direccion) from '\d+[\w\s]+\d+')
                        WHEN lower(easy.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                        regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easy.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                        else coalesce(substring(initcap(easy.direccion) from '^[^0-9]*[0-9]+'),initcap(easy.direccion))
                        END "Calle y Número",
                    CASE
                            WHEN easy.region='XIII - Metropolitana' THEN 'Region Metropolitana'
                            WHEN easy.region='V - Valparaíso' THEN 'Valparaíso'
                            else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))
                    END "Provincia/Estado",
                    CAST(easy.entrega AS varchar) AS "Código de Pedido",   -- Agrupar Por
                    easy.fecha_entrega AS "Fecha de Pedido",
                    easy.carton AS "Código de Producto",
                    easy.descripcion  AS "Descripción del Producto",
                    CASE 
                        WHEN easy.cant ~ '^\d+$' THEN (select count(*) 
                                                        from areati.ti_wms_carga_easy easy_a 
                                                        where easy_a.entrega = easy.entrega and easy_a.carton=easy.carton) 
                        -- Si el campo es solo un número
                        ELSE regexp_replace(easy.cant, '[^\d]', '', 'g')::numeric 
                        -- Si el campo contiene una frase con cantidad
                    END as "Cantidad de Producto",
                    cast(easy.producto as text) as "Cod. SKU",                            -- no va a Quadminds
                    easy.verified as "Pistoleado",
                    easy.nro_carga as "Carga",
                    easy.recepcion as "Recepcion",
                    initcap(lower(easy.comuna)) as "Comuna"              
            from areati.ti_wms_carga_easy easy
            --where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') 
            WHERE to_char(created_at,'yyyy-mm-dd hh24:mi')  >= to_char(('{dia_anterior}'::date + INTERVAL '17 hours 30 minutes'),'yyyy-mm-dd hh24:mi')
            AND to_char(created_at,'yyyy-mm-dd') <= to_char(CURRENT_DATE,'yyyy-mm-dd')
            --order by created_at desc
            order by easy.carton
                        """)           
            return cur.fetchall()
        
    ## buscar por codigo de productos
    
    def read_recepcion_electrolux_by_codigo_producto(self,codigo_producto):
        with self.conn.cursor() as cur:
            cur.execute(f"""
        select eltx.identificador_contacto AS "Código de Cliente",
        initcap(eltx.nombre_contacto) AS "Nombre",
        CASE 
        WHEN substring(initcap(split_part(eltx.direccion,',',1)) from '^\d') ~ '\d' then substring(initcap(split_part(eltx.direccion,',',1)) from '\d+[\w\s]+\d+')
        WHEN lower(split_part(eltx.direccion,',',1)) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN 
        regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '')
        else coalesce(substring(initcap(split_part(eltx.direccion,',',1)) from '^[^0-9]*[0-9]+'),eltx.direccion)
                END "Calle y Número",
                --initcap(split_part(eltx.direccion,',',2)) AS "Ciudad",
                --initcap(split_part(eltx.direccion,',',3)) AS "Provincia/Estado",
                case
                when unaccent(lower(eltx.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                (select oc.comuna_name from public.op_comunas oc 
                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(eltx.comuna))
            )
                )
                else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
            where unaccent(lower(eltx.comuna)) = unaccent(lower(oc2.comuna_name))
            )
        end as "Ciudad",
                CAST (eltx.numero_guia AS varchar) AS "Código de Pedido",
                eltx.fecha_min_entrega AS "Fecha de Pedido",
                CAST (eltx.codigo_item AS varchar) AS "Código de Producto",
                TRIM(eltx.nombre_item) AS "Descripción del Producto",
                cast(eltx.cantidad as numeric) AS "Cantidad de Producto",
            cast(eltx.codigo_item as text) as "Cod. SKU",					
            eltx.verified as "Pistoleado",
            eltx.recepcion as "Recepcion"
        from areati.ti_wms_carga_electrolux eltx
        where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') AND eltx.numero_guia = '{codigo_producto}'
                        """)           
            return cur.fetchall()
        
    def read_recepcion_easy_opl_by_codigo_producto(self,codigo_pedido):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             select  easygo.rut_cliente AS "Rut de Cliente",
             initcap(easygo.nombre_cliente) AS "Nombre",
             initcap(easygo.direc_despacho) AS "Calle y Número",
             initcap(easygo.comuna_despacho)  AS "Provincia/Estado",
             --coalesce(easygo.fono_cliente ,'0') AS "Teléfono",
             CAST (easygo.suborden AS varchar) AS "Código de Pedido",
             easygo.fec_compromiso AS "Fecha de Pedido",
             easygo.id_entrega AS "Código de Producto",
             easygo.descripcion AS "Descripción del Producto",
             cast(easygo.unidades as numeric) AS "Cantidad de Producto",
             easygo.codigo_sku as "Cod. SKU",                         
             easygo.verified as "Pistoleado",
             easygo.id_ruta as "N carga",
             easygo.recepcion as "Recepcion",
            'comuna' as "Comuna"
             
            from areati.ti_carga_easy_go_opl easygo
            --where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') AND easygo.suborden = '{codigo_pedido}'
            where easygo.suborden = '{codigo_pedido}' or easygo.id_entrega = '{codigo_pedido}'
            order by created_at desc          
                        """)           
            return cur.fetchall()

    def read_recepcion_sportex_by_codigo_producto(self,codigo_producto):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select twcs.id_sportex AS "Código de Cliente",
                initcap(twcs.cliente) as "Nombre",
                twcs.direccion as "Calle",
                twcs.comuna as "Provincia",
                CAST (twcs.id_sportex AS varchar) AS "Código de Pedido",
                twcs.fecha_entrega AS "Fecha de Pedido",
                twcs.id_sportex as "Codigo producto",
                coalesce(twcs.marca,'Sin Marca') AS "Descripción del Producto",
                1 AS "Cantidad de Producto",
                '' as "SKU",
                twcs.verified as "Pistoleado"
                from areati.ti_wms_carga_sportex twcs 
                where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') AND twcs.id_sportex = '{codigo_producto}'
                order by created_at desc
                        """)           
            return cur.fetchall()
        


    
        
    def read_recepcion_easy_cd_by_codigo_producto(self,codigo_producto):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                    select       CAST(easy.entrega AS varchar) AS "Código de Cliente",     
                            initcap(easy.nombre) AS "Nombre",
                            CASE 
                                WHEN substring(easy.direccion from '^\d') ~ '\d' then substring(initcap(easy.direccion) from '\d+[\w\s]+\d+')
                                WHEN lower(easy.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                                regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easy.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                                else coalesce(substring(initcap(easy.direccion) from '^[^0-9]*[0-9]+'),initcap(easy.direccion))
                                END "Calle y Número",
                            CASE
                                    WHEN easy.region='XIII - Metropolitana' THEN 'Region Metropolitana'
                                    WHEN easy.region='V - Valparaíso' THEN 'Valparaíso'
                                    else (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))
                            END "Provincia/Estado",
                            CAST(easy.entrega AS varchar) AS "Código de Pedido",   -- Agrupar Por
                            easy.fecha_entrega AS "Fecha de Pedido",
                            easy.carton AS "Código de Producto",
                            easy.descripcion  AS "Descripción del Producto",
                            CASE 
                                WHEN easy.cant ~ '^\d+$' THEN (select count(*) 
                                                                from areati.ti_wms_carga_easy easy_a 
                                                                where easy_a.entrega = easy.entrega and easy_a.carton=easy.carton) 
                                -- Si el campo es solo un número
                                ELSE regexp_replace(easy.cant, '[^\d]', '', 'g')::numeric 
                                -- Si el campo contiene una frase con cantidad
                            END as "Cantidad de Producto",
                            cast(easy.producto as text) as "Cod. SKU",                            -- no va a Quadminds
                            easy.verified as "Pistoleado",
                            easy.nro_carga as "Carga"
                    from areati.ti_wms_carga_easy easy
                    where easy.carton = '{codigo_producto}'
                    --order by created_at desc
                        """)           
            return cur.fetchall()

    def update_verified_recepcion(self,codigo_pedido, codigo_producto,cod_sku):
        sql_queries = [
            f"UPDATE areati.ti_wms_carga_sportex SET verified = true, recepcion = true  WHERE upper(areati.ti_wms_carga_sportex.id_sportex) = upper('{codigo_producto}')",
            f"UPDATE areati.ti_wms_carga_easy easy SET verified = true WHERE easy.entrega = '{codigo_pedido}' and easy.carton = '{codigo_producto}'",
            f"UPDATE areati.ti_wms_carga_electrolux eltx set verified = true, recepcion = true  where eltx.numero_guia = '{codigo_pedido}' and eltx.codigo_item = '{codigo_producto}'",
            f"UPDATE areati.ti_carga_easy_go_opl easygo SET verified = true where easygo.suborden = '{codigo_pedido}' AND easygo.codigo_sku = '{cod_sku}'"
        ]

        updates = []
        try:
            for query in sql_queries:
                with self.conn.cursor() as cur:
                    cur.execute(query)
                    # print(cur.rowcount)
                    updates.append(cur.rowcount)
                self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error durante la actualización:", error)
        finally:
            # pass
            return updates
        

    def update_estado_verified_opl(self,codigo_pedido,sku,check):
        with self.conn.cursor() as cur:
            cur.execute(f"""        
            UPDATE areati.ti_carga_easy_go_opl easygo 
            SET verified = {check}
            where easygo.suborden = '{codigo_pedido}' AND easygo.codigo_sku = '{sku}'
            """)
            row = cur.rowcount
        self.conn.commit()
        return row
    

    def update_verified_sportex(self,codigo_pedido):
        with self.conn.cursor() as cur:
            cur.execute(f"""        
                UPDATE areati.ti_wms_carga_sportex 
                SET verified = true, recepcion = true 
                WHERE upper(areati.ti_wms_carga_sportex.id_sportex) = upper('{codigo_pedido}')
            """)
            row = cur.rowcount
        self.conn.commit()
        return row

    def update_verified_electrolux(self,codigo_pedido):
        with self.conn.cursor() as cur:
            cur.execute(f"""        
                update areati.ti_wms_carga_electrolux eltx 
                set verified = true, recepcion = true 
                where eltx.numero_guia = '{codigo_pedido}'
            """)
            row = cur.rowcount
        self.conn.commit()

        return row


    def update_verified_opl(self,codigo_pedido,sku):
        with self.conn.cursor() as cur:
            cur.execute(f"""        
            UPDATE areati.ti_carga_easy_go_opl easygo 
            SET verified = true
            where easygo.suborden = '{codigo_pedido}' AND easygo.codigo_sku = '{sku}'
            """)
            row = cur.rowcount
        self.conn.commit()
        return row
    

    def update_recepcion_opl(self,codigo_pedido,sku):
        with self.conn.cursor() as cur:
            cur.execute(f"""        
            UPDATE areati.ti_carga_easy_go_opl easygo 
            SET recepcion = true  
            where easygo.suborden = '{codigo_pedido}' AND easygo.codigo_sku = '{sku}'
            """)
            row = cur.rowcount
        self.conn.commit()
        return row

    def update_verified_cd(self,codigo_producto):
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"""        
                UPDATE areati.ti_wms_carga_easy easy 
                SET verified = true
                WHERE easy.carton = '{codigo_producto}' or easy.entrega = '{codigo_producto}'
                """)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error durante la actualización:", error)
        finally:
            return cur.rowcount
        

    def update_verified_masivo_cd(self,codigos_producto):
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"""        
                UPDATE areati.ti_wms_carga_easy easy 
                SET verified = true
                WHERE easy.carton in ({(','.join([f"'{res}'" for res in codigos_producto]))}) or easy.entrega in ({(','.join([f"'{res}'" for res in codigos_producto]))})
                """,codigos_producto)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error durante la actualización:", error)
        finally:
            return cur.rowcount
    
    def update_recepcion_cd(self,codigo_producto):
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"""        
                UPDATE areati.ti_wms_carga_easy easy 
                SET recepcion = true
                WHERE easy.carton = '{codigo_producto}' or easy.entrega = '{codigo_producto}'
                """)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error durante la actualización:", error)
        finally:
            return cur.rowcount
        
    def get_codigo_pedido_opl(self, codigo):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                    SELECT substring('{codigo}', '\d+') 
                        """)
            return cur.fetchall()

    # Cargas por hora

    def get_carga_hora(self):
        with self.conn.cursor() as cur:
            # ### original oscar
            cur.execute("""
                with resumen_hora_oc as (
                    select 
                    "Fecha" as "Fecha",
                    "Hora Ingreso" as "Hora",
                    "id_cliente" as "Id_cliente",
                    "imagen_cliente" as "Imagen_cliente",
                    "N° Carga" as "Nro_carga",
                    "Entregas" as "Entregas",
                    "Bultos" as "Bultos",
                    "Verificados" as "Verificados",
                    "Sin Verificar"  as "No_verificados"
                    from areati.resumen_hora_productos_oc() 
                    )

                    select json_agg(resumen_hora_oc) from resumen_hora_oc
                        """)
            
            return cur.fetchone()

    def read_datos_descarga_beetrack(self, id_ruta : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""            
                select * from rutas.excel_a_beetrack_ruta('{id_ruta}');
                        """)
            return cur.fetchall()

    # Recuperar fecha ingreso sistema al cliente

    def recuperar_fecha_ingreso_cliente(self,cod_pedido):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                (select to_char(created_at, 'yyyy-mm-dd hh24:mi') from areati.ti_wms_carga_easy e where e.entrega='{cod_pedido}'
                union all
                select to_char(created_at, 'yyyy-mm-dd hh24:mi') from areati.ti_wms_carga_electrolux e where e.numero_guia='{cod_pedido}'  or trim(e.factura) = trim('{cod_pedido}')
                union all
                select to_char(created_at, 'yyyy-mm-dd hh24:mi') from areati.ti_wms_carga_sportex e where e.id_sportex='{cod_pedido}'
                union all
                select to_char(created_at, 'yyyy-mm-dd hh24:mi') from areati.ti_carga_easy_go_opl e where e.suborden='{cod_pedido}'
                union all
                select to_char(created_at, 'yyyy-mm-dd hh24:mi') from areati.ti_carga_easy_go_opl e where e.id_entrega='{cod_pedido}'
                limit 1
                                )
                        """)
            return cur.fetchall()
        
    
    # recuperar track de beetrack

    def recuperar_track_beetrack(self,codigo_pick):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select areati.recupera_tracking_beetrack('{codigo_pick}');
                        """)
            return cur.fetchall()
    
    def recupera_linea_producto(self,codigo_pick):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select areati.recupera_linea_producto('{codigo_pick}');
                        """)
            return cur.fetchall()
        
    def registrar_retiro_cliente(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO areati.ti_retiro_cliente
            (cliente, cod_pedido, tipo, envio_asociado, fecha_pedido, sku, descripcion, cantidad, bultos, nombre_cliente, direccion, comuna, telefono, email, estado, subestado, verified, ids_usuario)
            VALUES(%(Cliente)s, %(Codigo_pedido)s, %(Tipo)s, %(Envio_asociado)s, 
            %(Fecha_pedido)s, %(SKU)s, %(Descripcion_producto)s, %(Cantidad)s, %(Bultos)s,
            %(Nombre_cliente)s, %(Direccion)s, %(Comuna)s,%(Telefono)s, %(Email)s, 0, 0, true, %(Id_usuario)s);

            """,data)
        self.conn.commit()

    def find_retiro_cliente_existente(self,cod_pedido):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select estado from areati.ti_retiro_cliente trc where trc.cod_pedido = '{cod_pedido}'         
                        """)
            return cur.fetchall()
        
    def get_cargas_quadmind(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from quadminds.recupera_productos_a_quadminds();
                        """)
            return cur.fetchall()
        
    ##### funcion select * from quadminds.recupera_productos_a_quadminds(); por separado
        
    def get_cargas_quadmind_easy_cd(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            -------------------------------------------------------------------------------------------------------------------------------------
            -- EASY
            -------------------------------------------------------------------------------------------------------------------------------------
            select	CAST(easy.entrega AS varchar) AS "Código de Cliente",     
                    initcap(easy.nombre) AS "Nombre",
                    coalesce(tbm.direccion,
                    CASE 
                        WHEN substring(easy.direccion from '^\d') ~ '\d' then substring(initcap(easy.direccion) from '\d+[\w\s]+\d+')
                        WHEN lower(easy.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                        regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easy.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                        else coalesce(substring(initcap(easy.direccion) from '^[^0-9]*[0-9]+'),initcap(easy.direccion))
                    end) as "Calle y Número",
                    coalesce(tbm.comuna,
                    case
                        when unaccent(lower(easy.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select oc.comuna_name from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna))
                                                )
                            )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                            where unaccent(lower(easy.comuna)) = unaccent(lower(oc2.comuna_name))
                            )
                    end) as "Ciudad",
                    case
                        when unaccent(lower(easy.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select opr.region_name  from public.op_regiones opr 
                        where opr.id_region = (select oc.id_region from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                            where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna))
                            )	
                        ))
                        else(select opr.region_name  from public.op_regiones opr 
                        where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                        where unaccent(lower(easy.comuna)) = unaccent(lower(oc2.comuna_name))
                        ))
                    end as "Provincia/Estado",
                    '' AS "Latitud",
                    '' AS "Longitud", --7
                    coalesce(easy.telefono,'0') AS "Teléfono con código de país",
                    lower(easy.Correo) AS "Email",
                    CAST(easy.entrega AS varchar) AS "Código de Pedido",   -- Agrupar Por
                    coalesce(tbm.fecha,
                    CASE
                        WHEN twcep.fecha_entrega <> easy.fecha_entrega THEN twcep.fecha_entrega			-- Alertado por el Sistema
                        ELSE easy.fecha_entrega
                    END
                    ) AS "Fecha de Pedido",
                    'E' AS "Operación E/R",
                    (select string_agg(CAST(aux.carton AS varchar) , ' @ ') from areati.ti_wms_carga_easy aux
                    where aux.entrega = easy.entrega) AS "Código de Producto",
                    '(EASY) ' || (select string_agg(aux.descripcion , ' @ ') from areati.ti_wms_carga_easy aux
                                where aux.entrega = easy.entrega) AS "Descripción del Producto",
                    (select count(*) from areati.ti_wms_carga_easy easy_a where easy_a.entrega = easy.entrega) AS "Cantidad de Producto", --15
                    1 AS "Peso", --16
                    1 AS "Volumen",
                    1 AS "Dinero",
                    '8' AS "Duración min",
                    '09:00 - 21:00' AS "Ventana horaria 1",
                    '' AS "Ventana horaria 2",
                    'EASY CD' AS "Notas", -- 22
                    CASE
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))='Region Metropolitana' 
                        THEN 'RM' || ' - ' || coalesce (tts.tamano,'?')
                        WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))='Valparaíso' THEN 'V - ' ||  initcap(easy.comuna)
                        else 'S/A'
                    END "Agrupador",
                    '' AS "Email de Remitentes",
                    '' AS "Eliminar Pedido Si - No - Vacío",
                    '' AS "Vehículo",
                    '' AS "Habilidades"
            from areati.ti_wms_carga_easy easy
            left join public.ti_tamano_sku tts on tts.sku = cast(easy.producto as text)
            left join public.ti_wms_carga_easy_paso twcep on twcep.entrega = easy.entrega
            LEFT JOIN (
                SELECT DISTINCT ON (guia) guia as guia, 
                direccion_correcta as direccion, 
                comuna_correcta as comuna,
                fec_reprogramada as fecha,
                observacion,
                alerta
                FROM rutas.toc_bitacora_mae
                WHERE alerta = true
                ORDER BY guia, created_at desc
            ) AS tbm ON easy.entrega=tbm.guia
            where (easy.estado=0 or (easy.estado=2 and easy.subestado not in (7,10,12,13,19,43,44,50,51,70,80))) and easy.estado not in (1,3)
            and easy.entrega not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
            and easy.entrega not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
            -- and (easy.verified = true or easy.recepcion=true)
            group by easy.entrega,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20,21,22,23,24,25,26,27
    --limit 200 offset 200
                        """)
            return cur.fetchall()
        

    ## modificacion hecha por NEO
    def get_cargas_quadmind_easy_cd_mio(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            with auxi_easy_cd as (
            select string_agg(CAST(easy_a.carton AS varchar) , ' @ ') as "carton" ,
            string_agg(easy_a.descripcion , ' @ ') as "descripcion",
            count(*) as "cuenta",
            easy_a.entrega as "entrega"
            from areati.ti_wms_carga_easy easy_a 
            group by 4
        )

        -------------------------------------------------------------------------------------------------------------------------------------
        -- EASY
        -------------------------------------------------------------------------------------------------------------------------------------
        select	CAST(easy.entrega AS varchar) AS "Código de Cliente",     
                initcap(easy.nombre) AS "Nombre",
                coalesce(tbm.direccion,
                CASE 
                    WHEN substring(easy.direccion from '^\d') ~ '\d' then substring(initcap(easy.direccion) from '\d+[\w\s]+\d+')
                    WHEN lower(easy.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                    regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easy.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                    else coalesce(substring(initcap(easy.direccion) from '^[^0-9]*[0-9]+'),initcap(easy.direccion))
                end) as "Calle y Número",
                coalesce(tbm.comuna,
                case
                    when unaccent(lower(easy.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna))
                                            )
                        )
                    else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                        where unaccent(lower(easy.comuna)) = unaccent(lower(oc2.comuna_name))
                        )
                end) as "Ciudad",
                case
                    when unaccent(lower(easy.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                    (select opr.region_name  from public.op_regiones opr 
                    where opr.id_region = (select oc.id_region from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna))
                        )	
                    ))
                    else(select opr.region_name  from public.op_regiones opr 
                    where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                    where unaccent(lower(easy.comuna)) = unaccent(lower(oc2.comuna_name))
                    ))
                end as "Provincia/Estado",
                '' AS "Latitud",
                '' AS "Longitud", --7
                coalesce(easy.telefono,'0') AS "Teléfono con código de país",
                lower(easy.Correo) AS "Email",
                CAST(easy.entrega AS varchar) AS "Código de Pedido",   -- Agrupar Por
                coalesce(tbm.fecha,
                CASE
                    WHEN twcep.fecha_entrega <> easy.fecha_entrega THEN twcep.fecha_entrega			-- Alertado por el Sistema
                    ELSE easy.fecha_entrega
                END
                ) AS "Fecha de Pedido",
                'E' AS "Operación E/R",
                auxi.carton as  "Código de Producto",
                '(EASY) ' ||  auxi.descripcion AS "Descripción del Producto",
                auxi.cuenta AS "Cantidad de Producto", --15
                1 AS "Peso", --16
                1 AS "Volumen",
                1 AS "Dinero",
                '8' AS "Duración min",
                '09:00 - 21:00' AS "Ventana horaria 1",
                '' AS "Ventana horaria 2",
                'EASY CD' AS "Notas", -- 22
                CASE
                    WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))='Region Metropolitana' 
                    THEN 'RM' || ' - ' || coalesce (tts.tamano,'?')
                    WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))='Valparaíso' THEN 'V - ' ||  initcap(easy.comuna)
                    else 'S/A'
                END "Agrupador",
                '' AS "Email de Remitentes",
                '' AS "Eliminar Pedido Si - No - Vacío",
                '' AS "Vehículo",
                '' AS "Habilidades"
        from areati.ti_wms_carga_easy easy
        left join public.ti_tamano_sku tts on tts.sku = cast(easy.producto as text)
        left join public.ti_wms_carga_easy_paso twcep on twcep.entrega = easy.entrega
        left join auxi_easy_cd auxi on auxi.entrega = easy.entrega
        LEFT JOIN (
            SELECT DISTINCT ON (guia) guia as guia, 
            direccion_correcta as direccion, 
            comuna_correcta as comuna,
            fec_reprogramada as fecha,
            observacion,
            alerta
            FROM rutas.toc_bitacora_mae
            WHERE alerta = true
            ORDER BY guia, created_at desc
        ) AS tbm ON easy.entrega=tbm.guia
        where (easy.estado=0 or (easy.estado=2 and easy.subestado not in (7,10,12,13,19,43,44,50,51,70,80))) and easy.estado not in (1,3)
        and easy.entrega not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
        and easy.entrega not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
        -- and (easy.verified = true or easy.recepcion=true)
        group by easy.entrega,2,3,4,5,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
                        """)
            return cur.fetchall()
        
    
    def get_cargas_quadmind_easy_opl(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select  easygo.rut_cliente AS "Código de Cliente",
            initcap(easygo.nombre_cliente) AS "Nombre",
            coalesce(tbm.direccion,
            CASE 
                WHEN substring(easygo.direc_despacho from '^\d') ~ '\d' then substring(initcap(easygo.direc_despacho) from '\d+[\w\s]+\d+')
                WHEN lower(easygo.direc_despacho) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easygo.direc_despacho,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                else coalesce(substring(initcap(easygo.direc_despacho) from '^[^0-9]*[0-9]+'),initcap(easygo.direc_despacho))
            end) as "Calle y Número",
            coalesce(tbm.comuna,
            case
                when unaccent(lower(easygo.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                    (select oc.comuna_name from public.op_comunas oc 
                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easygo.comuna_despacho))
                                        )
                    )
                else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                    where unaccent(lower(easygo.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                    )
            end) as "Ciudad",
            case
                when unaccent(lower(easygo.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                (select opr.region_name  from public.op_regiones opr 
                where opr.id_region = (select oc.id_region from public.op_comunas oc 
                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easygo.comuna_despacho))
                    )	
                ))
                else(select opr.region_name  from public.op_regiones opr 
                where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                where unaccent(lower(easygo.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                ))
            end as "Provincia/Estado",
            '' AS "Latitud",
            '' AS "Longitud",
            coalesce(easygo.fono_cliente ,'0') AS "Teléfono con código de país",
            lower(easygo.correo_cliente) AS "Email",
            CAST (easygo.suborden AS varchar) AS "Código de Pedido",
            coalesce(tbm.fecha,easygo.fec_compromiso) AS "Fecha de Pedido",
            'E' AS "Operación E/R",
            --easygo.id_entrega AS "Código de Producto",
            (select string_agg(CAST(aux.codigo_sku AS varchar) , ' @ ') from areati.ti_carga_easy_go_opl aux
            where aux.suborden = easygo.suborden) AS "Código de Producto",
            '(Easy OPL) ' || (select string_agg(aux.descripcion , ' @ ') from areati.ti_carga_easy_go_opl aux
            where aux.suborden = easygo.suborden) AS "Descripción del Producto",
            (select count(*) from areati.ti_carga_easy_go_opl easy_a where easy_a.suborden = easygo.suborden) AS "Cantidad de Producto",
            1 AS "Peso",
            1 AS "Volumen",
            1 AS "Dinero",
            '8' AS "Duración min",
            '09:00 - 21:00' AS "Ventana horaria 1",
            '' AS "Ventana horaria 2",
            coalesce((select 'Easy OPL - ' || se.name from areati.subestado_entregas se where easygo.subestado=se.code),'Easy OPL') AS "Notas",
            CASE
            WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easygo.comuna_despacho)))='Region Metropolitana' 
            THEN 'RM' || ' - ' || coalesce (tts.tamano,'?') 
            WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easygo.comuna_despacho)))='Valparaíso' THEN 'V - ' ||  initcap(easygo.comuna_despacho)
            else 'S/A'
    END "Agrupador",
            '' AS "Email de Remitentes",
            '' AS "Eliminar Pedido Si - No - Vacío",
            '' AS "Vehículo",
            '' AS "Habilidades"
    from areati.ti_carga_easy_go_opl easygo
    left join ti_comuna_region tcr on
        translate(lower(easygo.comuna_despacho),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
    left join public.ti_tamano_sku tts on tts.sku = cast(easygo.codigo_sku as text)
    LEFT JOIN (
        SELECT DISTINCT ON (guia) guia as guia, 
        direccion_correcta as direccion, 
        comuna_correcta as comuna,
        fec_reprogramada as fecha,
        observacion,
        alerta
        FROM rutas.toc_bitacora_mae
        WHERE alerta = true
        ORDER BY guia, created_at desc
    ) AS tbm ON easygo.suborden=tbm.guia
    where (easygo.estado=0 or (easygo.estado=2 and easygo.subestado not in (7,10,12,13,19,43,44,50,51,70,80))) and easygo.estado not in (1,3)
    and easygo.suborden not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
    and easygo.suborden not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
    -- and (easygo.verified = true or easygo.recepcion=true)
    group by easygo.suborden,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20,21,22,23,24,25,26,27,1
                        """)
            return cur.fetchall()
    
    # hecho por Loki

    def get_cargas_quadmind_easy_opl_mio(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            with auxi_easy_opl as (
            select string_agg(CAST(easy_opl_a.codigo_sku AS varchar) , ' @ ') as "codigo_pedido" ,
            string_agg(easy_opl_a.descripcion , ' @ ') as "descripcion",
            count(*) as "cuenta",
            easy_opl_a.suborden as "entrega"
            from areati.ti_carga_easy_go_opl easy_opl_a 
            group by 4
        )
        
        select  easygo.rut_cliente AS "Código de Cliente",
                initcap(easygo.nombre_cliente) AS "Nombre",
                coalesce(tbm.direccion,
                CASE 
                    WHEN substring(easygo.direc_despacho from '^\d') ~ '\d' then substring(initcap(easygo.direc_despacho) from '\d+[\w\s]+\d+')
                    WHEN lower(easygo.direc_despacho) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                    regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easygo.direc_despacho,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                    else coalesce(substring(initcap(easygo.direc_despacho) from '^[^0-9]*[0-9]+'),initcap(easygo.direc_despacho))
                end) as "Calle y Número",
                coalesce(tbm.comuna,
                case
                    when unaccent(lower(easygo.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easygo.comuna_despacho))
                                            )
                        )
                    else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                        where unaccent(lower(easygo.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                        )
                end) as "Ciudad",
                case
                    when unaccent(lower(easygo.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                    (select opr.region_name  from public.op_regiones opr 
                    where opr.id_region = (select oc.id_region from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easygo.comuna_despacho))
                        )	
                    ))
                    else(select opr.region_name  from public.op_regiones opr 
                    where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                    where unaccent(lower(easygo.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                    ))
                end as "Provincia/Estado",
                '' AS "Latitud",
                '' AS "Longitud",
                coalesce(easygo.fono_cliente ,'0') AS "Teléfono con código de país",
                lower(easygo.correo_cliente) AS "Email",
                CAST (easygo.suborden AS varchar) AS "Código de Pedido",
                coalesce(tbm.fecha,easygo.fec_compromiso) AS "Fecha de Pedido",
                'E' AS "Operación E/R",
                --easygo.id_entrega AS "Código de Producto",
                --(select string_agg(CAST(aux.codigo_sku AS varchar) , ' @ ') from areati.ti_carga_easy_go_opl aux
                --where aux.suborden = easygo.suborden) AS "Código de Producto",
                auxi.codigo_pedido as  "Código de Producto",
                --'(Easy OPL) ' || (select string_agg(aux.descripcion , ' @ ') from areati.ti_carga_easy_go_opl aux
            -- where aux.suborden = easygo.suborden) AS "Descripción del Producto",
                '(Easy OPL) ' ||  auxi.descripcion AS "Descripción del Producto",
                --(select count(*) from areati.ti_carga_easy_go_opl easy_a where easy_a.suborden = easygo.suborden) AS "Cantidad de Producto",
                auxi.cuenta AS "Cantidad de Producto", --15
                1 AS "Peso",
                1 AS "Volumen",
                1 AS "Dinero",
                '8' AS "Duración min",
                '09:00 - 21:00' AS "Ventana horaria 1",
                '' AS "Ventana horaria 2",
                coalesce((select 'Easy OPL - ' || se.name from areati.subestado_entregas se where easygo.subestado=se.code),'Easy OPL') AS "Notas",
                CASE
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easygo.comuna_despacho)))='Region Metropolitana' 
                THEN 'RM' || ' - ' || coalesce (tts.tamano,'?') 
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easygo.comuna_despacho)))='Valparaíso' THEN 'V - ' ||  initcap(easygo.comuna_despacho)
                else 'S/A'
        END "Agrupador",
                '' AS "Email de Remitentes",
                '' AS "Eliminar Pedido Si - No - Vacío",
                '' AS "Vehículo",
                '' AS "Habilidades"
        from areati.ti_carga_easy_go_opl easygo
        left join ti_comuna_region tcr on
            translate(lower(easygo.comuna_despacho),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
        left join public.ti_tamano_sku tts on tts.sku = cast(easygo.codigo_sku as text)
        left join auxi_easy_opl auxi on auxi.entrega = easygo.suborden
        LEFT JOIN (
            SELECT DISTINCT ON (guia) guia as guia, 
            direccion_correcta as direccion, 
            comuna_correcta as comuna,
            fec_reprogramada as fecha,
            observacion,
            alerta
            FROM rutas.toc_bitacora_mae
            WHERE alerta = true
            ORDER BY guia, created_at desc
        ) AS tbm ON easygo.suborden=tbm.guia
        where (easygo.estado=0 or (easygo.estado=2 and easygo.subestado not in (7,10,12,13,19,43,44,50,51,70,80))) and easygo.estado not in (1,3)
        and easygo.suborden not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
        and easygo.suborden not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
        -- and (easygo.verified = true or easygo.recepcion=true)
        group by easygo.suborden,2,3,4,5,6,7,8,9,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,1
                        """)
            return cur.fetchall()
      

    def get_cargas_quadmind_electrolux(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select 	eltx.identificador_contacto AS "Código de Cliente",
            initcap(eltx.nombre_contacto) AS "Nombre",
            coalesce(tbm.direccion,
            CASE 
                WHEN substring(initcap(split_part(eltx.direccion,',',1)) from '^\d') ~ '\d' then substring(initcap(split_part(eltx.direccion,',',1)) from '\d+[\w\s]+\d+')
                WHEN lower(split_part(eltx.direccion,',',1)) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN 
                -- cast(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.*$', ''), '\s+(\d+\D+\d+).*$', ' \1') AS varchar)
                -- REPLACE(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', '')
                regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '')
                else coalesce(substring(initcap(split_part(eltx.direccion,',',1)) from '^[^0-9]*[0-9]+'),eltx.direccion)
            end) as "Calle y Número",
            coalesce(tbm.comuna,
            case
                when unaccent(lower(eltx.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                    (select oc.comuna_name from public.op_comunas oc 
                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(eltx.comuna))
                                        )
                    )
            else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                where unaccent(lower(eltx.comuna)) = unaccent(lower(oc2.comuna_name))
                )
            end) as "Ciudad",
            case
                when unaccent(lower(eltx.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                (select opr.region_name  from public.op_regiones opr 
                where opr.id_region = (select oc.id_region from public.op_comunas oc 
                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(eltx.comuna))
                    )	
                ))
                else(select opr.region_name  from public.op_regiones opr 
                where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                where unaccent(lower(eltx.comuna)) = unaccent(lower(oc2.comuna_name))
                ))
            end as "Provincia/Estado",
            eltx.latitud AS "Latitud",
            eltx.longitud AS "Longitud",
            coalesce(eltx.telefono,'0') AS "Teléfono con código de país",
            lower(eltx.email_contacto) AS "Email",
            CAST (eltx.numero_guia AS varchar) AS "Código de Pedido",
            coalesce(tbm.fecha,
            case
                when calculo.fecha_siguiente <> eltx.fecha_min_entrega
                then calculo.fecha_siguiente
                else eltx.fecha_min_entrega
            end) AS "Fecha de Pedido",
            'E' AS "Operación E/R",
            (select string_agg(CAST(aux.codigo_item AS varchar) , ' @ ') from areati.ti_wms_carga_electrolux aux
            where aux.numero_guia = eltx.numero_guia) AS "Código de Producto",
            '(Electrolux) ' || (select string_agg(aux.nombre_item , ' @ ') from areati.ti_wms_carga_electrolux aux
    
            where aux.numero_guia = eltx.numero_guia) AS "Descripción del Producto",
            (select count(*) from areati.ti_wms_carga_electrolux eltx_a where eltx_a.numero_guia = eltx.numero_guia) as "Cantidad de Producto",
            1 AS "Peso",
            1 AS "Volumen",
            1 AS "Dinero",
            '8' AS "Duración min",
            '09:00 - 21:00' AS "Ventana horaria 1",
            '' AS "Ventana horaria 2",
            coalesce((select 'Electrolux - ' || se.name from areati.subestado_entregas se where eltx.subestado=se.code),'Electrolux') AS "Notas",
            --'RM - ' ||  initcap(eltx.comuna)  AS agrupador,
            CASE
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(eltx.comuna)))='Region Metropolitana' 
                THEN 'RM' || ' - ' || coalesce (tts.tamano,'?')
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(eltx.comuna)))='Valparaíso' 
                THEN 'V - ' ||  initcap(eltx.comuna)
                else 'S/A'
            END "Agrupador",
            '' AS "Email de Remitentes",
            '' AS "Eliminar Pedido Si - No - Vacío",
            '' AS "Vehículo",
            '' AS "Habilidades"
    from areati.ti_wms_carga_electrolux eltx
    left join public.ti_tamano_sku tts on tts.sku = cast(eltx.codigo_item as text)
    LEFT JOIN (
        SELECT DISTINCT ON (guia) guia as guia, 
        direccion_correcta as direccion, 
        comuna_correcta as comuna,
        fec_reprogramada as fecha,
        observacion,
        alerta
        FROM rutas.toc_bitacora_mae
        WHERE alerta = true
        ORDER BY guia, created_at desc
    ) AS tbm ON eltx.numero_guia=tbm.guia
    left join (
        select distinct on (numero_guia)
        numero_guia, 
        to_date(to_char(created_at,'yyyy-mm-dd'),'yyyy-mm-dd') as ingreso,
        fecha_min_entrega,
        CASE
        WHEN EXTRACT(ISODOW FROM created_at + INTERVAL '1 day') = 7 THEN to_date(to_char(created_at + INTERVAL '3 days','yyyy-mm-dd'),'yyyy-mm-dd')
        ELSE to_date(to_char(created_at + INTERVAL '2 days','yyyy-mm-dd'),'yyyy-mm-dd')
        END AS fecha_siguiente
        from areati.ti_wms_carga_electrolux   
    ) as calculo on calculo.numero_guia = eltx.numero_guia
    where (eltx.estado=0 or (eltx.estado=2 and eltx.subestado not in (7,10,12,13,19,43,44,50,51,70,80))) and eltx.estado not in (1,3)
    and eltx.numero_guia not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
    and eltx.numero_guia not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
    -- and (eltx.verified = true or eltx.recepcion=true)
    group by eltx.numero_guia,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20,21,22,23,24,25,26,27,1
                        """)
            return cur.fetchall()

    def get_cargas_quadmind_sportex(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select	twcs.id_sportex AS "Código de Cliente",
            initcap(twcs.cliente) AS "Nombre",
            coalesce(tbm.direccion,
            CASE 
                WHEN substring(initcap(replace(twcs.direccion,',','')) from '^\d') ~ '\d' then substring(initcap(replace(twcs.direccion,',','')) from '\d+[\w\s]+\d+')
                WHEN lower(twcs.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN 
                regexp_replace(regexp_replace(initcap(twcs.direccion), ',.*$', ''), '\s+(\d+\D+\d+).*$', ' \1')
                else substring(initcap(replace(twcs.direccion,',','')) from '^[^0-9]*[0-9]+')
            end) as "Calle y Número",
            coalesce(tbm.direccion,
            case
                when unaccent(lower(twcs.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                    (select oc.comuna_name from public.op_comunas oc 
                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twcs.comuna))
                                        )
                    )
            else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                where unaccent(lower(twcs.comuna)) = unaccent(lower(oc2.comuna_name))
                )
            end) as "Ciudad",
            case
                when unaccent(lower(twcs.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                (select opr.region_name  from public.op_regiones opr 
                where opr.id_region = (select oc.id_region from public.op_comunas oc 
                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twcs.comuna))
                    )	
                ))
                else(select opr.region_name  from public.op_regiones opr 
                where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                where unaccent(lower(twcs.comuna)) = unaccent(lower(oc2.comuna_name))
                ))
            end as "Provincia/Estado",
            '' AS "Latitud",
            '' AS "Longitud",
            coalesce(twcs.fono,'0') AS "Teléfono con código de país",
            lower(twcs.correo) AS "Email",
            CAST (twcs.id_sportex AS varchar) AS "Código de Pedido",
            --coalesce(tbm.fecha,cast(twcs.created_at as date) + 2) AS "Fecha de Pedido",
            coalesce(tbm.fecha,twcs.fecha_entrega) AS "Fecha de Pedido",
            'E' AS "Operación E/R",
            twcs.id_sportex AS "Código de Producto",
            '(Sportex) ' || coalesce(twcs.marca,'Sin Marca') AS "Descripción del Producto", 
            1 AS "Cantidad de Producto",
            1 AS "Peso",
            1 AS "Volumen",
            1 AS "Dinero",
            '8' AS "Duración min",
            '09:00 - 21:00' AS "Ventana horaria 1",
            '' AS "Ventana horaria 2",
            coalesce((select 'Sportex - ' || se.name from areati.subestado_entregas se where twcs.subestado=se.code),'Sportex') AS "Notas",
            CASE
            WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(twcs.comuna)))='Region Metropolitana' THEN 'RM - P' 
            WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(twcs.comuna)))='Valparaíso' THEN 'V - ' ||  initcap(twcs.comuna)
            else 'S/A'
    END "Agrupador",
            '' AS "Email de Remitentes",
            '' AS "Eliminar Pedido Si - No - Vacío",
            '' AS "Vehículo",
            '' AS "Habilidades"
    from areati.ti_wms_carga_sportex twcs
    left join ti_comuna_region tcr on
        translate(lower(twcs.comuna),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
    LEFT JOIN (
        SELECT DISTINCT ON (guia) guia as guia, 
        direccion_correcta as direccion, 
        comuna_correcta as comuna,
        fec_reprogramada as fecha,
        observacion,
        alerta
        FROM rutas.toc_bitacora_mae
        WHERE alerta = true
        ORDER BY guia, created_at desc
    ) AS tbm ON twcs.id_sportex=tbm.guia
    where (twcs.estado=0 or (twcs.estado=2 and twcs.subestado not in (7,10,12,13,19,43,44,50,51,70,80))) and twcs.estado not in (1,3)
    and twcs.id_sportex not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
    and twcs.id_sportex not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                        """)
            return cur.fetchall()

    def get_cargas_quadmind_retiro_cliente(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select  trc.envio_asociado AS "Código de Cliente",
            initcap(trc.nombre_cliente) AS "Nombre",
            CASE 
                WHEN substring(trc.direccion from '^\d') ~ '\d' then substring(initcap(trc.direccion) from '\d+[\w\s]+\d+')
                WHEN lower(trc.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(trc.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                else coalesce(substring(initcap(trc.direccion) from '^[^0-9]*[0-9]+'),initcap(trc.direccion)) 
            END "Calle y Número",
            coalesce(tbm.comuna,
            case
                when unaccent(lower(trc.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                (select oc.comuna_name from public.op_comunas oc 
                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(trc.comuna))
                )
                )
                else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                where unaccent(lower(trc.comuna)) = unaccent(lower(oc2.comuna_name))
                )
            end) as "Ciudad",
            case
                when unaccent(lower(trc.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                (select opr.region_name  from public.op_regiones opr 
                where opr.id_region = (select oc.id_region from public.op_comunas oc 
                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(trc.comuna))
                    )    
                ))
                else(select opr.region_name  from public.op_regiones opr 
                where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                where unaccent(lower(trc.comuna)) = unaccent(lower(oc2.comuna_name))
                ))
            end as "Provincia/Estado",
            '' AS "Latitud",
            '' AS "Longitud",
            coalesce(trc.telefono ,'0') AS "Teléfono con código de país",
            lower(trc.email) AS "Email",
            CAST (trc.cod_pedido AS varchar) AS "Código de Pedido",
            coalesce(tbm.fecha,trc.fecha_pedido) AS "Fecha de Pedido",
            'E' AS "Operación E/R",
            (select string_agg(CAST(aux.sku AS varchar) , ' @ ') from areati.ti_retiro_cliente aux
            where aux.cod_pedido = trc.cod_pedido) AS "Código de Producto",
            '(' || trc.cliente ||') ' || (select string_agg(aux.descripcion , ' @ ') from areati.ti_retiro_cliente aux
    
            where aux.cod_pedido = trc.cod_pedido) AS "Descripción del Producto",
    
            (select count(*) from areati.ti_retiro_cliente trc_a where trc_a.cod_pedido = trc.cod_pedido) AS "Cantidad de Producto",
            1 AS "Peso",
            1 AS "Volumen",
            1 AS "Dinero",
            '8' AS "Duración min",
            '09:00 - 21:00' AS "Ventana horaria 1",
            '' AS "Ventana horaria 2",
            coalesce((select trc.cliente || ' - ' || se.name from areati.subestado_entregas se where trc.subestado=se.code),trc.cliente) AS "Notas",
            CASE
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(trc.comuna)))='Region Metropolitana' 
                THEN 'RM' || ' - ' || coalesce (tts.tamano,'?') 
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(trc.comuna)))='Valparaíso' THEN 'V - ' ||  initcap(trc.comuna)
                else 'S/A'
            END "Agrupador",
            '' AS "Email de Remitentes",
            '' AS "Eliminar Pedido Si - No - Vacío",
            '' AS "Vehículo",
            '' AS "Habilidades"
    from areati.ti_retiro_cliente trc
    left join ti_comuna_region tcr on
        translate(lower(trc.comuna),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
    left join public.ti_tamano_sku tts on tts.sku = cast(trc.sku as text)
    LEFT JOIN (
        SELECT DISTINCT ON (guia) guia as guia, 
        direccion_correcta as direccion, 
        comuna_correcta as comuna,
        fec_reprogramada as fecha,
        observacion,
        alerta
        FROM rutas.toc_bitacora_mae
        WHERE alerta = true
        ORDER BY guia, created_at desc
    ) AS tbm on trc.cod_pedido=tbm.guia
    where (trc.estado=0 or (trc.estado=2 and trc.subestado not in (7,10,12,13,19,43,44,50,51,70,80))) and trc.estado not in (1,3)
    and trc.cod_pedido not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
    and trc.cod_pedido not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
    -- and (trc.verified = true or trc.recepcion=true)
    group by trc.cod_pedido,2,3,4,5,6,7,8,9,11,12,16,17,18,19,20,21,22,23,24,25,26,27,1,trc.cliente
                        """)
            return cur.fetchall()
    
    #### Aqui termina funcion select * from quadminds.recupera_productos_a_quadminds(); por separado
    def get_cargas_quadmind_offset(self,offset):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from quadminds.recupera_productos_a_quadminds_offset({offset})
                        """)
            return cur.fetchall()

    def get_pedidos_planificados_quadmind(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from quadminds.pedidos_planificados
            where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
                        """)
            return cur.fetchall()
        
    def get_pedido_planificados_quadmind_by_cod_pedido(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select count(*) from quadminds.pedidos_planificados
            where cod_pedido = '2907132378'
                        """)
            return cur.fetchone()
        

    ## NS_beetrack por rango de fecha

    def get_NS_beetrack_por_rango_fecha(self,fecha_inicio,fecha_termino):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from areati.recupera_ns_beetrack('{fecha_inicio}','{fecha_termino}');
                        """)
            return cur.fetchall()
        
    ## Insertar datos latitud longitud

    def insert_latlng(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO rutas.latlng (id_usuario, direccion, comuna, region, lat, lng, ids_usuario, display_name, "type")
            VALUES(%(Id_usuario)s, %(Direccion)s, %(Comuna)s, %(Region)s, %(Lat)s, %(Lng)s, %(Ids_usuario)s, %(Display_name)s, %(Type)s);
            """,data)
        self.conn.commit() 


    ## checar si la direccion ya esta registrada

    def check_direccion_existe(self, direccion):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select count(*) from rutas.latlng where direccion = '{direccion}'
                        """)
            return cur.fetchone()
        
    def buscar_producto_toc(self, codigo,id_cliente):
        with self.conn.cursor() as cur:

            print(f"from rutas.toc_buscar_producto('{codigo}',{id_cliente})")
            cur.execute(f"""
           with producto_toc as (
                select
                        "Fecha" as "Fecha" ,
                        "Patente" as "Patente" ,
                        "Guia" as "Guia",
                        "Cliente" as "Cliente",
                        "id_cliente" as "Id_cliente",
                        "Region" as "Region",
                        "Estado" as "Estado",
                        "Subestado" as "Subestado",
                        "Usuario Movil" as "Usuario_movil",
                        "Nombre Cliente" as "Nombre_cliente",
                        "F. Compromiso" as "Fecha_compromiso",
                        "Comuna" as "Comuna",
                        "Correo" as "Correo",
                        "Teléfono" as "Telefono"
                from rutas.toc_buscar_producto('{codigo}',{id_cliente})
                )
                select json_agg(producto_toc) from producto_toc
                        """)
            return cur.fetchone() 
        
    def buscar_subestados(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from areati.subestado_entregas se 
            where parent_code in (1,2)
            order by 2, 4
                        """)
            return cur.fetchall() 
        
    def buscar_codigos1(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT id, descripcion, descripcion_larga
            FROM rutas.def_codigo1;
                        """)
            return cur.fetchall() 
        
    def id_transyanez_bitacora(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from rutas.genera_siguiente_id_transyanez(); 
                        """)
            return cur.fetchone() 

    def insert_bitacora_toc(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO rutas.toc_bitacora_mae
(fecha, ppu, guia, cliente, region, estado, subestado, driver, nombre_cliente, fec_compromiso, comuna, direccion_correcta, comuna_correcta, fec_reprogramada, observacion, subestado_esperado, id_transyanez, ids_transyanez, id_usuario, ids_usuario, alerta, codigo1,id_cliente)
VALUES( %(Fecha)s, %(PPU)s, %(Guia)s, %(Cliente)s, %(Region)s, %(Estado)s, %(Subestado)s, %(Driver)s, %(Nombre_cliente)s, %(Fecha_compromiso)s, %(Comuna)s, %(Direccion_correcta)s, %(Comuna_correcta)s
      , %(Fecha_reprogramada)s, %(Observacion)s, %(Subestado_esperado)s, %(Id_transyanez)s, %(Ids_transyanez)s, %(Id_usuario)s, %(Ids_usuario)s, %(Alerta)s, %(Codigo1)s,%(Cliente_id_ty)s);
            """,data)
        self.conn.commit()

    def obtener_observaciones_usuario(self, ids_usuario):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select tbm.guia as "Guía", 
            tbm.cliente as "Cliente", 
            tbm.comuna as "Comuna", 
            coalesce(to_char(tbm.fec_compromiso,'yyyy-mm-dd'),'Sin fecha compromiso') as "Fec. Comp.",
            --tbm.fec_compromiso as "Fec. Comp.", 
            coalesce(to_char(tbm.fec_reprogramada ,'yyyy-mm-dd'),'Sin fecha reprogramada') as "Fec. Reprog.",
            --tbm.fec_reprogramada "Fec. Reprog.",
            tbm.observacion as "Observación", 
            tbm.ids_transyanez as "Código TY", 
            tbm.alerta as "Alerta",
            coalesce(trb.identificador,null) as en_ruta
            from rutas.toc_bitacora_mae tbm
            left join quadminds.ti_respuesta_beetrack trb on trb.guia = tbm.guia
            where to_char(tbm.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            and ids_usuario = '{ids_usuario}'
            order by tbm.created_at desc
                        """)
            return cur.fetchall()
        
    def obtener_alertas_vigentes_v1(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select to_char(tbm.created_at,'yyyy-mm-dd') as "Fec. Creación", 
            tbm.guia as "Guía", 
            tbm.cliente as "Cliente", 
            coalesce(coalesce(tbm.comuna_correcta || '*',tbm.comuna), 'sin Info.') as "Comuna", 
            coalesce(tbm.direccion_correcta || '*',  (select "Calle y Número" from areati.busca_ruta_manual_base2(tbm.guia) limit 1)) as "Dirección",
            coalesce(to_char(tbm.fec_reprogramada,'yyyy-mm-dd') || '*', to_char(tbm.fec_compromiso,'yyyy-mm-dd')) as "Fec. Comp.", 
            tbm.observacion as "Observación", 
            tbm.ids_transyanez as "Código TY", 
            tbm.alerta as "Alerta",
            coalesce(trb.identificador,null) as en_ruta,
            tbm.id
            from rutas.toc_bitacora_mae tbm
            left join quadminds.ti_respuesta_beetrack trb on trb.guia = tbm.guia
            where tbm.alerta = true
            ORDER BY
            CASE
                WHEN tbm.fec_reprogramada < CURRENT_DATE THEN 0  -- Atrasadas
                WHEN tbm.fec_reprogramada = CURRENT_DATE THEN 1  -- Hoy
                ELSE 2                                           -- Adelantadas
            END,
            tbm.fec_reprogramada asc, tbm.fec_compromiso asc;
 
                        """)
            return cur.fetchall()
    
    def obtener_alertas_vigentes(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            --select * from areati.busca_ruta_manual_base2('1374380901')
            with direccion_pedido as (
            select  distinct on (easy.carton)        
                    coalesce(
                    tbm.direccion,
                    CASE 
                        WHEN substring(easy.direccion from '^\d') ~ '\d' then substring(initcap(easy.direccion) from '\d+[\w\s]+\d+')
                        WHEN lower(easy.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                        regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easy.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                        else coalesce(substring(initcap(easy.direccion) from '^[^0-9]*[0-9]+'),initcap(easy.direccion))
                    end) as "Calle y Número",
                    coalesce(tbm.direccion,easy.direccion) as "Dirección Textual",
                    CAST(easy.entrega AS varchar) AS "Código de Pedido"   -- Agrupar Por
                        
            from areati.ti_wms_carga_easy easy
            LEFT JOIN (
                SELECT DISTINCT ON (guia) guia as guia, 
                direccion_correcta as direccion, 
                comuna_correcta as comuna,
                fec_reprogramada as fecha,
                observacion,
                alerta
                FROM rutas.toc_bitacora_mae
                WHERE alerta = true
                ORDER BY guia, created_at desc
            ) AS tbm ON easy.entrega=tbm.guia
            -------------------------------------------------------------------------------------------------------------------------------------
            -- ELECTROLUX
            -- [22/09/2023] Se incorpora Alerta por sistema para productos que tienen condición de NO RETORNO A RUTA
            -------------------------------------------------------------------------------------------------------------------------------------
            union all
                select 
                    coalesce(tbm.direccion,
                    CASE 
                WHEN substring(initcap(split_part(eltx.direccion,',',1)) from '^\d') ~ '\d' then substring(initcap(split_part(eltx.direccion,',',1)) from '\d+[\w\s]+\d+')
                WHEN lower(split_part(eltx.direccion,',',1)) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN 
                -- cast(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.*$', ''), '\s+(\d+\D+\d+).*$', ' \1') AS varchar)
                -- REPLACE(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', '')
                regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '')
                else coalesce(substring(initcap(split_part(eltx.direccion,',',1)) from '^[^0-9]*[0-9]+'),eltx.direccion)
                    end) as "Calle y Número",
                    coalesce(tbm.direccion,eltx.direccion) as "Dirección Textual",
                    CAST (eltx.numero_guia AS varchar) AS "Código de Pedido"                                      -- Alertado por el Sistema
            from areati.ti_wms_carga_electrolux eltx
            LEFT JOIN (
                SELECT DISTINCT ON (guia) guia as guia, 
                direccion_correcta as direccion, 
                comuna_correcta as comuna,
                fec_reprogramada as fecha,
                observacion,
                alerta
                FROM rutas.toc_bitacora_mae
                WHERE alerta = true
                ORDER BY guia, created_at desc
            ) AS tbm ON eltx.numero_guia=tbm.guia

            -------------------------------------------------------------------------------------------------------------------------------------
            -- SPORTEX
            -- [22/09/2023] Se incorpora Alerta por sistema para productos que tienen condición de NO RETORNO A RUTA
            -------------------------------------------------------------------------------------------------------------------------------------
            -------------------------------------------------------------------------------------------------------------------------------------
            -- Easy OPL
            -- [22/09/2023] Se incorpora Alerta por sistema para productos que tienen condición de NO RETORNO A RUTA
            -------------------------------------------------------------------------------------------------------------------------------------
            union all  
            select
                    coalesce(tbm.direccion,
                    CASE 
                        WHEN substring(easygo.direc_despacho from '^\d') ~ '\d' then substring(initcap(easygo.direc_despacho) from '\d+[\w\s]+\d+')
                        WHEN lower(easygo.direc_despacho) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                        regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easygo.direc_despacho,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                        else coalesce(substring(initcap(easygo.direc_despacho) from '^[^0-9]*[0-9]+'),initcap(easygo.direc_despacho))
                    end) as "Calle y Número",
                    coalesce(tbm.direccion,easygo.direc_despacho) as "Dirección Textual",
                    CAST (easygo.suborden AS varchar) AS "Código de Pedido"                                      -- Alertado por el Sistema
            from areati.ti_carga_easy_go_opl easygo
            LEFT JOIN (
                SELECT DISTINCT ON (guia) guia as guia, 
                direccion_correcta as direccion, 
                comuna_correcta as comuna,
                fec_reprogramada as fecha,
                observacion,
                alerta
                FROM rutas.toc_bitacora_mae
                WHERE alerta = true
                ORDER BY guia, created_at desc
            ) AS tbm ON easygo.suborden=tbm.guia

            -------------------------------------------------------------------------------------------------------------------------------------
            -- (5) Retiro Cliente [24/07/2023]
            -- [22/09/2023] Se incorpora Alerta por sistema para productos que tienen condición de NO RETORNO A RUTA
            -------------------------------------------------------------------------------------------------------------------------------------
            union all
            select  --retc.envio_asociado AS "Código de Cliente",
                    coalesce(tbm.direccion,
                    CASE 
                        WHEN substring(retc.direccion from '^\d') ~ '\d' then substring(initcap(retc.direccion) from '\d+[\w\s]+\d+')
                        WHEN lower(retc.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                        regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(retc.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                        else coalesce(substring(initcap(retc.direccion) from '^[^0-9]*[0-9]+'),initcap(retc.direccion))
                    end) as "Calle y Número",
                    coalesce(tbm.direccion,retc.direccion) as "Dirección Textual",
                    CAST (retc.cod_pedido AS varchar) AS "Código de Pedido"
                -- Alertado por el Sistema
            from areati.ti_retiro_cliente retc
            LEFT JOIN (
                SELECT DISTINCT ON (guia) guia as guia, 
                direccion_correcta as direccion, 
                comuna_correcta as comuna,
                fec_reprogramada as fecha,
                observacion,
                alerta
                FROM rutas.toc_bitacora_mae
                WHERE alerta = true
                ORDER BY guia, created_at desc
            ) AS tbm on retc.cod_pedido=tbm.guia
            ) 

            select distinct  on (tbm.guia)
                    to_char(tbm.created_at,'yyyy-mm-dd') as "Fec. Creación", 
                        tbm.guia as "Guía", 
                        tbm.cliente as "Cliente", 
                        coalesce(coalesce(tbm.comuna_correcta || '*',tbm.comuna), 'sin Info.') as "Comuna", 
                        coalesce(tbm.direccion_correcta || '*',  dp."Calle y Número") as "Dirección",
                        coalesce(to_char(tbm.fec_reprogramada,'yyyy-mm-dd') || '*', to_char(tbm.fec_compromiso,'yyyy-mm-dd')) as "Fec. Comp.", 
                        tbm.observacion as "Observación", 
                        tbm.ids_transyanez as "Código TY", 
                        tbm.alerta as "Alerta",
                        coalesce(trb.identificador,null) as en_ruta,
                        tbm.id
                        from rutas.toc_bitacora_mae tbm
                        left join quadminds.ti_respuesta_beetrack trb on trb.guia = tbm.guia
                        left join direccion_pedido dp on dp."Código de Pedido" = tbm.guia
                        where tbm.alerta = true
                        ORDER by 2,
                        CASE
                            WHEN tbm.fec_reprogramada < CURRENT_DATE THEN 0  -- Atrasadas
                            WHEN tbm.fec_reprogramada = CURRENT_DATE THEN 1  -- Hoy
                            ELSE 2                                           -- Adelantadas
                        END,
                        tbm.fec_reprogramada asc, tbm.fec_compromiso asc;
            
                        """)
            return cur.fetchall()
        
    def update_alerta_bitacora_toc_by_guia(self, guia : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""        
            UPDATE rutas.toc_bitacora_mae
            SET alerta = false
            WHERE guia = '{guia}'
            """)
            row = cur.rowcount
        self.conn.commit()
        return row


    def obtener_nombres_usu_toc(self,fecha_inicio: str, fecha_fin: str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select count(id_usuario) as "Cantidad bitacoras", ids_usuario
            from rutas.toc_bitacora_mae 
            where to_char(created_at,'yyyymmdd')>='{fecha_inicio}'
			and to_char(created_at,'yyyymmdd')<='{fecha_fin}'
            group by id_usuario, ids_usuario
                        """)
            return cur.fetchall()
        
    def bitacoras_rango_fecha(self, fecha_inicio : str, fecha_fin : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             select * from rutas.listar_bitacora_fechas('{fecha_inicio}','{fecha_fin}')
                        """)
            return cur.fetchall()
        
    def actividad_diaria_usuario(self, ids_usuario : str, fecha : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             select * from rutas.toc_actividad_diaria('{ids_usuario}', '{fecha}');
                        """)
            return cur.fetchall()
        
    def toc_backoffice_usuario(self, ids_usuario : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             select * from rutas.toc_backoffice_usuario('{ids_usuario}');
                        """)
            return cur.fetchall()
        
    def read_rutas_pendientes_rango_fecha(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
             select * from rutas.pendientes(%(Fecha_inicio)s,%(Fecha_fin)s);
                        """,data)
            return cur.fetchall()
        
    def read_toc_tracking(self,cod_producto):
        with self.conn.cursor() as cur:
            cur.execute(f"""
              select * from rutas.toc_tracking('{cod_producto}')
              order by 1 desc
                        """)
            return cur.fetchall()
        
    def get_alerta_carga_ruta_activa(self,nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             select cod_pedido ,cod_producto, posicion, alerta, desc_producto  from quadminds.datos_ruta_manual drm 
            where nombre_ruta = '{nombre_ruta}'
            order by posicion
                        """)
            return cur.fetchall()
        

    def read_guia_toc(self,codigo):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT guia
                FROM rutas.toc_bitacora_mae
                where guia = '{codigo}' or ids_transyanez = '{codigo}'
                limit 1
                        """)
            return cur.fetchone()
        

    ## Editar subestado esperado
    #        

    def update_subestado_esperado(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""        
            UPDATE rutas.toc_bitacora_mae
            SET created_at = current_timestamp, subestado_esperado = %(Subestado_esperado)s , observacion = %(Observacion)s, alerta = %(Alerta)s , fec_reprogramada = %(Fecha_reprogramada)s
            , direccion_correcta = %(Direccion_correcta)s, comuna_correcta = %(Comuna_correcta)s, codigo1 = %(Codigo1)s
            where ids_transyanez = %(Ids_transyanez)s
            """, data)
            row = cur.rowcount
        self.conn.commit()
        return row    
    
    def buscar_alerta_by_ids_transyanez(self, ids_ty):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select tbm.alerta,  
            coalesce(to_char(tbm.fec_reprogramada,'yyyy-mm-dd'), to_char(tbm.fec_compromiso,'yyyy-mm-dd')) as "Fec. Comp.", 
            coalesce(tbm.direccion_correcta ,  (select "Calle y Número" from areati.busca_ruta_manual_base2(tbm.guia) limit 1)) as "Dirección",
            coalesce(tbm.comuna_correcta ,tbm.comuna) as "Comuna", 
            tbm.subestado, tbm.subestado_esperado, tbm.observacion, tbm.codigo1 ,tbm.guia
            from  rutas.toc_bitacora_mae as tbm
            where ids_transyanez  = '{ids_ty}' limit 1
                        """)
            return cur.fetchall()
        

    
        
    def armar_rutas_bloque(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
                select * from rutas.armar_ruta_bloque(%(Codigos)s, %(Fecha_ruta)s, %(Id_user)s)
                        """,data)
            return cur.fetchall()
            

    ##Actualizar en_stock a true codigo antiguo RSV
        

    def actualizar_stock_true_rsv(self,codigo_bar):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           UPDATE rsv.etiquetas 
           SET en_stock=true 
           WHERE bar_code = '{codigo_bar}'
            """)
            row = cur.rowcount
        self.conn.commit()
        
        return row
    
    ##Actualizar en_stock a false codigo nuevo RSV
        

    def actualizar_stock_false_rsv(self,codigo_bar):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           UPDATE rsv.etiquetas 
           SET en_stock=false 
           WHERE bar_code = '{codigo_bar}'
            """)
            row = cur.rowcount
        self.conn.commit()
        
        return row
    
    ##Actualizar despacho  a codigo nuevo RSV
        

    def actualizar_despacho_codigo_nuevo_rsv(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
           ----actualizarlo del despacho

            UPDATE rsv.despacho
            SET created_at = current_date, id_usuario = %(Id_user)s , 
                ids_usuario = %(Ids_user)s, id_etiqueta = %(Id_etiqueta)s,
                bar_code = %(Bar_code_nuevo)s
            WHERE bar_code =  %(Bar_code_antiguo)s and id_nota_venta = %(Id_nota_venta)s
            """,data)
            row = cur.rowcount
        self.conn.commit()
        
        return row
            

    ## New challenger  RSV


    def read_reporte_etiquetas_rsv(self,sucursal_id):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select 	e.carga,
                        e.bar_code,
                        e.codigo, 
                        e.descripcion,
                        e.color,
                        e.tipo,
                        e.en_stock,
                        e.ubicacion 
                from rsv.etiquetas e
                where e.carga in (    select distinct on (nombre_carga)
                                nombre_carga 
                                from rsv.cargas
                                where sucursal in (
                                    select distinct on (c.sucursal) c.sucursal
                                    from rsv.cargas c
                                    left join rsv.sucursal s on c.sucursal = s.id
                                    where sucursal={sucursal_id}
                                ))
                and e.en_stock = true
                order by color, codigo, bar_code
                limit 1000
                        """)
            return cur.fetchall()
        

    def read_reporte_etiquetas_rsv_excel(self,sucursal_id):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select 	e.carga,
                        e.bar_code,
                        e.codigo, 
                        e.descripcion,
                        e.color,
                        e.tipo,
                        case 
                        	when e.en_stock = true then 'En Stock'
                        	else 'Sin Stock'
                        end as en_stock,
                        e.ubicacion 
                from rsv.etiquetas e
                where e.carga in (    select distinct on (nombre_carga)
                                nombre_carga 
                                from rsv.cargas
                                where sucursal in (
                                    select distinct on (c.sucursal) c.sucursal
                                    from rsv.cargas c
                                    left join rsv.sucursal s on c.sucursal = s.id
                                    where sucursal={sucursal_id}
                                ))
                and e.en_stock = true
                order by color, codigo, bar_code
                        """)
            return cur.fetchall()


    def update_preparado_nota_venta_rsv(self, id_nota_venta,fecha_preparado):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            update rsv.nota_venta
            set preparado = true, fecha_preparado = '{fecha_preparado}'
            where id = {id_nota_venta}
            """)
            row = cur.rowcount
        self.conn.commit()
        
        return row

    def read_catalogo_rsv(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT id, created_at, codigo, producto, unid_x_paquete, peso, ancho, alto, largo, color, coalesce(precio_unitario,0), ubicacion_p, ubicacion_u, codigo_original, id_user, ids_user, habilitado, unid_con_etiqueta
                FROM rsv.catalogo_productos
                order by 1;
                        """)
            return cur.fetchall()
        
    def read_catalogo_by_color_rsv(self,color):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select codigo, producto,color,unid_con_etiqueta  from rsv.catalogo_productos 
                where color = {color}
                        """)
            return cur.fetchall()
        

    def read_catalogo_by_color_sin_filtro_rsv(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select distinct (codigo), producto, color, unid_con_etiqueta  from rsv.catalogo_productos 
                --where color = 2
                order by color
                        """)
            return cur.fetchall()
        
    def buscar_producto_existente_rsv(self,codigo):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select codigo from rsv.catalogo_productos where codigo = '{codigo}'
                        """)
            return cur.fetchone()
        
    # def buscar_producto_por_codigo_rsv(self,codigo):
    #     with self.conn.cursor() as cur:
    #         cur.execute(f"""
    #             select * from rsv.catalogo_productos where codigo = '{codigo}'
    #                     """)
    #         return cur.fetchall()

    def insert_nuevo_catalogo_rsv(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO rsv.catalogo_productos
            (codigo, producto, unid_x_paquete, peso, ancho, alto, largo, id_user, ids_user, color, codigo_original, precio_unitario, ubicacion_p, ubicacion_u)
            VALUES( %(Codigo_final)s, %(Producto)s, %(Unid_x_paquete)s, %(Peso)s,%(Ancho)s,%(Alto)s,%(Largo)s,%(Id_user)s, %(Ids_user)s,%(Color)s,%(Codigo)s, %(Precio_unitario)s, %(Ubicacion_p)s, %(Ubicacion_u)s );
            """,data)
        self.conn.commit()

    def update_catalogo_rsv(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""

            UPDATE rsv.catalogo_productos
            SET producto= %(Producto)s , unid_x_paquete = %(Unid_x_paquete)s , peso = %(Peso)s, ancho = %(Ancho)s, 
                alto = %(Alto)s, largo = %(Largo)s, id_user = %(Id_user)s, ids_user = %(Ids_user)s, codigo_original=%(Codigo)s, precio_unitario = %(Precio_unitario)s
                -- ,ubicacion_p = %(Ubicacion_p)s, ubicacion_u = %(Ubicacion_u)s
            WHERE codigo = %(Codigo_final)s            

            """,data)
        self.conn.commit()


    def read_colores_rsv(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM rsv.colores;
                        """)
            return cur.fetchall()
        
    def read_cargas_rsv(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM rsv.cargas order by 1;
                        """)
            return cur.fetchall()
        

    def read_cargas_por_nombre_carga_rsv(self,nombre_carga):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select *  from rsv.cargas c where trim(nombre_carga) = trim('{nombre_carga}')
                        """)
            return cur.fetchall()
        
    def read_lista_carga_rsv(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                select distinct (nombre_carga), etiquetas  FROM rsv.cargas 
                        """)
            return cur.fetchall()
        

    def delete_cargas(self,array : str,nombre_carga : str):
        codigos = array.split(',')
        valores = array 
        with self.conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM rsv.cargas WHERE codigo IN ({','.join(['%s']*len(codigos))}) AND nombre_carga = '{nombre_carga}'
                        """, codigos)
            rows_delete = cur.rowcount
        self.conn.commit() 
        return rows_delete
    
    def check_codigo_existente_carga(self, nombre_carga, codigo):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select codigo  from rsv.cargas c where nombre_carga = '{nombre_carga}' and codigo = '{codigo}'
                        """)
            return cur.fetchall()



    # def update_producto_carga(self,nombre_carga, codigo):

    def read_lista_carga_rsv_por_mes(self,mes):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select distinct (nombre_carga), etiquetas, fecha_ingreso, su.nombre  FROM rsv.cargas 
                inner join rsv.sucursal su on sucursal = su.id
                where to_char(fecha_ingreso ,'yyyymm')= '{mes}' 
                order by fecha_ingreso desc
                        """)
            return cur.fetchall()
        
    def update_carga_rsv(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            UPDATE rsv.cargas
            SET id_usuario=%(Id_user)s, ids_usuario=%(Ids_user)s, paquetes=%(Paquetes)s, unidades=%(Unidades)s
            WHERE nombre_carga  = %(Nombre_carga)s and codigo = %(Codigo)s
            """,data)
        self.conn.commit()
        
    def read_cargas_por_color_rsv(self,color):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select * from rsv.cargas where color = {color}
                        """)
            return cur.fetchall()
        
    ## asigna ubicacion por codigo de barra
    def read_cargas_rsv_porId(self, barCode):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select bar_code, descripcion, codigo, ubicacion, verificado from rsv.etiquetas e where bar_code = '{barCode}';
                        """)
            return cur.fetchall()
    def update_carga_rsv_porId(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE  rsv.etiquetas
                        SET ubicacion= %(Ubicacion)s, verificado=%(verificado)s, en_stock = true WHERE bar_code = %(bar_code)s;
            """, data)   
            self.conn.commit()  


    def read_sucursal(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select * from rsv.sucursal;    
                        """)
            return cur.fetchall()

    def read_sucursal_porId(self, id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT id, nombre FROM rsv.sucursal WHERE id = %s;", (id,))
            return cur.fetchall()

    def read_sucursal_match(self, barCode):
        with self.conn.cursor() as cur:
            cur.execute(f""" select * from rsv.busca_sucursal_barcode('{barCode}')
                    """)
            return cur.fetchall()


    ##  fin asigna ubicacion por codigo de barra
        

    ##generar etiquetas

    def generar_etiquitas_rsv(self,nombre_carga):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select * from rsv.generar_etiquetas('{nombre_carga}');
                        """)
            return cur.fetchall()

    ## mostrar datos de productos guardados en etiquetas

    def read_datos_productos_etiquetas_rsv(self,nombre_carga):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select distinct (codigo),descripcion, color FROM rsv.etiquetas e where carga = '{nombre_carga}' 
                        """)
            return cur.fetchall()
        


    def read_etiquetas_rsv(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT * FROM rsv.etiquetas order by 1
                        """)
            return cur.fetchall()
    

    ## Insert carga
    def insert_carga_rsv(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
           INSERT INTO rsv.cargas
           (fecha_ingreso, id_usuario, ids_usuario, nombre_carga, codigo, color, paquetes, unidades, verificado, sucursal)
           VALUES(%(Fecha_ingreso)s, %(Id_user)s, %(Ids_user)s, %(Nombre_carga)s, %(Codigo)s, %(Color)s, %(Paquetes)s, %(Unidades)s, false, %(Sucursal)s);        
         """,data)
        self.conn.commit()

    def buscar_cargas_rsv(self, nombre_carga):
        with self.conn.cursor() as cur:
            cur.execute(f"""
              select distinct (nombre_carga) from rsv.cargas where nombre_carga = '{nombre_carga}'
                        """)
            return cur.fetchone()
        
    def obtener_etiqueta_carga_rsv(self,nombre_carga : str , codigo : str, tipo : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
              select * from rsv.obtener_etiqueta_carga_tipo('{nombre_carga}','{codigo}', '{tipo}');
                        """)
            return cur.fetchall()
        

    def obtener_inventario_por_sucursal(self, sucursal : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
              --select * from rsv.inventario_por_sucursal({sucursal});
              select 	cp.codigo,
                        c.nombre_color ,
                        cp.producto,
                        inv.e_paquetes as "Paquetes",
                        inv.e_unidades as "Unidades",
                        inv.total as "Total Producto",
                        (select array_agg(distinct(substring(ubicacion from '([^@]+)@' )))
                        from rsv.etiquetas 
                        where codigo = cp.codigo AND substring(ubicacion from '@(\d+)' ) = '{sucursal}') as ubicacion
                from rsv.catalogo_productos cp 
                JOIN LATERAL rsv.existencia(cp.codigo,{sucursal}) AS inv ON true
                left join rsv.colores c on cp.color = c.id
                where habilitado=true
                order by cp.color, cp.codigo asc
                -- limit 1000
              
                        """)
            return cur.fetchall()


    def obtener_ubicacion_cantidad(self, sucursal : int, codigo : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select * from rsv.obtener_ubicacion_cantidad('{codigo}',{sucursal}) 
                        """)
            return cur.fetchall() 


    def obtener_stock_de_producto_de_sucursal(self, sucursal : int, producto : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select * from rsv.existencia('{producto}',{sucursal})
                        """)
            return cur.fetchone()   
    
    def obtener_inventario_por_sucursal_excel(self, sucursal : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            --select * from rsv.inventario_por_sucursal({sucursal});     
            select 	cp.codigo,
                        c.nombre_color ,
                        cp.producto,
                        inv.e_paquetes as "Paquetes",
                        inv.e_unidades as "Unidades",
                        inv.total as "Total Producto",
                        (select string_agg(distinct(substring(ubicacion from '([^@]+)@' )), ' - ')
                        from rsv.etiquetas 
                        where codigo = cp.codigo AND substring(ubicacion from '@(\d+)' ) = '{sucursal}') as ubicacion
                from rsv.catalogo_productos cp 
                JOIN LATERAL rsv.existencia(cp.codigo,{sucursal}) AS inv ON true
                left join rsv.colores c on cp.color = c.id
                where habilitado=true
                order by cp.color, cp.codigo asc;  
                    
                        """)
            return cur.fetchall()


    def read_tipo_despacho_rsv(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
              SELECT id, despacho FROM rsv.tipo_despacho;
                        """)
            return cur.fetchall()
        
    ## Obtener sucursales rsv

    def read_sucursales_rsv(self):
        with self.conn.cursor() as cur:
            cur.execute("""
              select id, nombre, pais, ciudad, comuna ,direccion, latitud,longitud, id_usuario ,ids_usuario 
              FROM rsv.sucursal
                        """)
            return cur.fetchall()

    ## obtener datos carga por nombre carga

    def read_datos_carga_por_nombre_rsv(self,nombre_carga : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
              select * from rsv.obtener_datos_carga('{nombre_carga}');
                        """)
            return cur.fetchall()
        
    ## obtener datos carga por nombre carga para descargar datos de excel

    def read_datos_carga_por_nombre_rsv_descarga_excel(self,nombre_carga : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""          
            select fecha_ingreso , nombre_color, codigo , producto , paquetes , unidades , total_unidades , cuenta_p , verifica_p , cuenta_u , verifica_u  
            from rsv.obtener_datos_carga('{nombre_carga}')
                        """)
            return cur.fetchall()
    
    ## Ventas
    # ingresar nota_venta

    def insert_nota_venta_rsv(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO rsv.nota_venta
            (id_usuario, ids_usuario, sucursal, cliente, direccion, comuna, region, fecha_entrega, 
             tipo_despacho, numero_factura, codigo_ty, preparado, entregado)
            VALUES(%(Id_user)s, %(Ids_user)s,%(Sucursal)s, %(Cliente)s, %(Direccion)s, %(Comuna)s, 
                   %(Region)s, %(Fecha_entrega)s,%(Tipo_despacho)s, %(Numero_factura)s , %(Codigo_ty)s,false, false);        
         """,data)
        self.conn.commit()

    def insert_nota_venta_producto_rsv(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO rsv.nota_venta_productos
            (id_venta, codigo, unidades, id_usuario, ids_usuario)
            VALUES(%(Id_venta)s, %(Codigo)s,%(Unidades)s, %(Id_user)s,%(Ids_user)s);      
         """,data)
        self.conn.commit()


    def get_max_id_nota_venta(self) :
        with self.conn.cursor() as cur:
            cur.execute("""
            select coalesce (max(id)+1,1) from rsv.nota_venta nv 
            """)

            return cur.fetchone()
        
    def evaluar_pedido_unidad(self,data) :
        with self.conn.cursor() as cur:
            cur.execute("""
            select * from rsv.evaluar_pedido_unidad(%(Codigo_producto)s,%(Cantidad)s,%(Sucursal)s)
            """,data)
            return cur.fetchone()
            

    def obtener_codigo_factura_venta(self) :
        with self.conn.cursor() as cur:
            cur.execute("""
            select * from rsv.generar_codigo_factura();            
            """)
            return cur.fetchone()
        

    def obtener_estructuras_rsv(self) :
        with self.conn.cursor() as cur:
            cur.execute("""
            select e.nombre , e.sucursal , te.tipo , e.cant_espacios , e.balanceo, e.frontis
            from rsv.estructuras e
            left join rsv.tipo_estructura te on te.id = e.tipo 
            where e.tipo = 1
            order by e.nombre;       
            """)
            return cur.fetchall()
        
    def obtener_tipo_estructuras_rsv(self) :
        with self.conn.cursor() as cur:
            cur.execute("""
            select * from rsv.tipo_estructura te        
            """)
            return cur.fetchall()
        

    def read_lista_ventas_por_mes(self,mes : str,sucursal : int) :
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select nv.id, nv.created_at, nv.id_usuario, nv.ids_usuario, nv.sucursal, nv.cliente, nv.direccion, nv.comuna,
                   nv.region, nv.fecha_entrega, td.despacho, nv.numero_factura, nv.codigo_ty ,nv.preparado , nv.fecha_preparado ,nv.entregado , nv.fecha_despacho
            from rsv.nota_venta nv
            inner join rsv.tipo_despacho td  on td.id = nv.tipo_despacho 
            where to_char(created_at  ,'yyyymm')= '{mes}' and sucursal = {sucursal} 
            order by nv.fecha_entrega         
            """)
            return cur.fetchall()
        
    def read_lista_ventas_rsv(self, sucursal : int) :
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select nv.id, nv.created_at, nv.id_usuario, nv.ids_usuario, nv.sucursal, nv.cliente, nv.direccion, nv.comuna,
                   nv.region, nv.fecha_entrega, td.despacho, nv.numero_factura, nv.codigo_ty ,nv.preparado , nv.fecha_preparado ,nv.entregado , nv.fecha_despacho
            from rsv.nota_venta nv
            inner join rsv.tipo_despacho td  on td.id = nv.tipo_despacho 
            where sucursal = {sucursal} 
            order by nv.fecha_entrega         
            """)
            return cur.fetchall()
    
    def obtener_lista_detalle_ventas_barcode_rsv(self, id_venta : int) :
        with self.conn.cursor() as cur:
            cur.execute(f"""
            
            select * from rsv.detalle_venta_barcode({id_venta})  
            """)
            return cur.fetchall()
        
    def obtener_lista_detalle_ventas_rsv(self, id_venta : int) :
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT nvp.id, nvp.id_venta, nvp.codigo, nvp.unidades, cp.producto 
            FROM rsv.nota_venta_productos nvp
            inner join rsv.catalogo_productos cp on cp.codigo = nvp.codigo 
            where id_venta = {id_venta}
            order by nvp.codigo desc
            """)
            return cur.fetchall()
        
    def obtener_cantidad_producto_actual_rsv(self, id_venta : int) :
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select distinct(e.codigo) as codigo_producto , sum(
            case 
                when e.tipo = 'U' then 1
                when e.tipo = 'P' and (select unid_con_etiqueta  
                from RSV.catalogo_productos cp where cp.codigo = e.codigo) = true then
                (select unid_x_paquete  
                from RSV.catalogo_productos cp where cp.codigo = e.codigo) * des.cantidad
                when e.tipo = 'P' and (select unid_con_etiqueta  
                from RSV.catalogo_productos cp where cp.codigo = e.codigo) = false then des.cantidad
            end) as "total" 
            from rsv.despacho as des 
            left join rsv.etiquetas e on des.bar_code  = e.bar_code 
            where des.id_nota_venta  = {id_venta}
            group by e.codigo

           -- select distinct(e.codigo) as codigo_producto , sum(des.cantidad) as "total" 
           -- from rsv.despacho as des 
           -- left join rsv.etiquetas e on des.bar_code  = e.bar_code 
           -- where des.id_nota_venta  = {id_venta}
           -- group by 1
           -- order by 1

           -- SELECT  DISTINCT split_part( (REGEXP_MATCHES(bar_code , '^(.*?)@', 'g'))[1] , '-', 1 )as codigo_producto, sum(cantidad) as "total" 
           -- from rsv.despacho as des where des.id_nota_venta  =  {id_venta}
           -- group by 1
           -- order by 1
            """)
            return cur.fetchall()

        
    
    def obtener_nota_ventas_rsv(self, id_venta : int) :
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select nv.id, nv.created_at, nv.id_usuario, nv.ids_usuario, nv.sucursal, nv.cliente, nv.direccion, nv.comuna,
                   nv.region, nv.fecha_entrega, td.despacho, nv.numero_factura, nv.codigo_ty ,nv.preparado , nv.fecha_preparado ,nv.entregado , nv.fecha_despacho
            from rsv.nota_venta nv
            inner join rsv.tipo_despacho td  on td.id = nv.tipo_despacho 
            where  nv.id = {id_venta}
            order by nv.fecha_entrega 
            """)
            return cur.fetchone()
        
    ### update estado entrega de nota venta
    def update_estado_entrega_nota_venta_rsv(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            UPDATE rsv.nota_venta
            SET entregado=true , fecha_despacho = %(Fecha_despacho)s
            WHERE id= %(Id_venta)s;
            """,data)
            row = cur.rowcount
        self.conn.commit()
        
        return row
    
    ## generar codigo factura de notas de ventas

    def generar_codigo_factura_nota_venta_rsv(self, tipo_retiro : int) :
        with self.conn.cursor() as cur:
            cur.execute(f"""
             select * from rsv.generar_codigo_factura ({tipo_retiro}); 
            """)
            return cur.fetchone()
        
    # Obtener peso_posicion_succursal
    def peso_posicion_sucursal(self,estructura,sucursal):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             --select * from rsv.peso_posicion_suc('{estructura}',{sucursal}) 
              select codigo,tipo,cantidad , coalesce (peso_total , 0) as peso_total from rsv.peso_posicion_suc('{estructura}',{sucursal}) 
            """)
            return cur.fetchall()

    def insert_data_despacho_rsv(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rsv.despacho (id_usuario,ids_usuario, id_nota_venta, id_etiqueta, bar_code, latitud, longitud, cantidad) 
                VALUES(%(Id_user)s, %(Ids_user)s, %(Id_nota_venta)s, %(Id_etiqueta)s, %(Bar_code)s, %(Lat)s , %(Lng)s, %(Cantidad)s); 
                        """,data)
        self.conn.commit()



        
    ##Y

    def read_paquetes_abiertos(self, sucursal : int):
         with self.conn.cursor() as cur:
             cur.execute(f"""
                         select fecha, paquetes.id, paquetes.carga, paquetes.bar_code,
                         paquetes.codigo, color, descripcion, tipo from rsv.paquetes_abiertos_sucursal('{sucursal}') AS paquetes
                         INNER JOIN rsv.etiquetas AS e ON paquetes.id = e.id;
                """)

             return cur.fetchall()

    def reimprimir_etiqueta_paquete_abierto_rsv(self, codigo : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
              select * from rsv.etiqueta_paquete_abierto('{codigo}');
                        """)
            return cur.fetchall()

    def reimprimir_etiqueta_unica_rsv(self,nombre_carga : str , codigo : str, tipo : str, bar_code:str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
              select * from rsv.obtener_etiqueta_carga_tipo('{nombre_carga}','{codigo}', '{tipo}') where bar_code = '{bar_code}';
                        """)
            return cur.fetchall()

    def abrir_paquete_nuevo_rsv(self, bar_code: str):
        with self.conn.cursor() as cur:
            cur.execute(f"""  select * from rsv.abrir_paquete('{bar_code}');
                 """)
            return cur.fetchall()

    ##bitacora rsv 
    def insert_data_bitacora_rsv(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rsv.bitacora_producto (id_usuario, ids_usuario, sucursal, id_etiqueta, 
                        bar_code, momento, lat, lng) VALUES(%(id_usuario)s, %(ids_usuario)s, %(sucursal)s,
                         %(id_etiqueta)s, %(bar_code)s, %(momento)s, %(lat)s, %(lng)s); 
                        """,data)

        self.conn.commit()

    ###/Y


    ##Suma de peso de las estructuras RSV


    def obtener_suma_estructuras(self, lados, estructuras, sucursal):
        with self.conn.cursor() as cur:

            query = "SELECT SUM(peso_total) FROM ("
            for lado in lados:
                query+= f"SELECT peso_total FROM rsv.peso_posicion_suc('{estructuras}{lado}', {sucursal}) UNION ALL "

            query = query[:-10] + ") as peso_total"

            cur.execute(query)
            return cur.fetchone()[0]

    def obtener_suma_estructura_E13_E18(self, estructura: str , sucursal : int):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                   select sum (peso_total)
                    from
                    (select peso_total from rsv.peso_posicion_suc('{estructura}18',{sucursal})
                    union all
                    select peso_total from rsv.peso_posicion_suc('{estructura}16',{sucursal})
                    union all
                    select peso_total from rsv.peso_posicion_suc('{estructura}15',{sucursal})
                    union all
                    select peso_total from rsv.peso_posicion_suc('{estructura}14',{sucursal})
                    union all
                    select sum(peso_total) from rsv.peso_posicion_suc('{estructura}13',{sucursal})
                    ) as peso
                 """)
            return cur.fetchone()
        

    def obtener_suma_estructura_E9_E19(self, estructura: str, sucursal : int):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                    select sum (peso_total)
                    from
                    (select peso_total from rsv.peso_posicion_suc('{estructura}19',{sucursal})
                    union all
                    select peso_total from rsv.peso_posicion_suc('{estructura}12',{sucursal})
                    union all
                    select peso_total from rsv.peso_posicion_suc('{estructura}11',{sucursal})
                    union all
                    select peso_total from rsv.peso_posicion_suc('{estructura}10',{sucursal})
                    union all
                    select peso_total from rsv.peso_posicion_suc('{estructura}9',{sucursal})
                    ) as peso
                 """)
            return cur.fetchone()
        
    
### update estado entrega de nota venta
    def update_estructura_rsv(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            UPDATE rsv.estructuras
            SET  sucursal=%(Sucursal)s, tipo=%(Tipo)s, cant_espacios=%(Cant_espacios)s, balanceo=%(Balanceo)s, frontis=%(Frontis)s
            WHERE nombre = %(Nombre)s
            """,data)
            row = cur.rowcount
        self.conn.commit()
        
        return row
    
    ## update stock etiqueta
    def update_stock_etiqueta_rsv(self, bar_cod):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           UPDATE rsv.etiquetas 
            SET en_stock=false 
            WHERE bar_code = '{bar_cod}'
            """)
            row = cur.rowcount
        self.conn.commit()
        
        return row
    
    ## obtener unidades por paquete

    def obtener_unidades_por_paquete(self, codigo : str):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                    select unid_x_paquete, unid_con_etiqueta from rsv.catalogo_productos cp where codigo = '{codigo}'
                 """)
            return cur.fetchone()
        

    def obtener_etiquetas_de_paquete(self, id_paquete : str):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                    select bar_code from rsv.etiquetas where split_part(split_part(bar_code, '@',2), '-',1 ) = '{id_paquete}'
                    and en_stock = true 
                    order by posicion 
                 """)
            return cur.fetchall()
        
    ## obtener unidades por paquete

    def verificar_stock_paquete(self, bar_cod : str):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                    select id,en_stock  from rsv.etiquetas e where bar_code = '{bar_cod}'
                 """)
            return cur.fetchone()
        

    def armar_venta_rsv(self, id_nota_venta : int, sucursal):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                    select * from rsv.armar_venta({id_nota_venta}, {sucursal}) 
                 """)
            return cur.fetchall()   
        
    ## obtener el cod por si los prros ponen un - en el codigo

    def obtener_codigo_por_bar_code(self, bar_cod : str):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                    select codigo from rsv.etiquetas e where bar_code = '{bar_cod}'
                 """)
            return cur.fetchone()

    #### beetrack 
    ## Distpatch guide
    def insert_beetrack_dispatch_guide_update(self, data):
            with self.conn.cursor() as cur:
                cur.execute("""
                INSERT INTO beetrack.dispatch_guide_update
                (resource, evento, identifier, truck_identifier, status, substatus, substatus_code, contact_identifier, arrived_at, latitud, longitud, bultos, comuna, driver, item_name, item_quantity, item_delivered_quantity, item_code)
                VALUES( %(resource)s, %(event)s, %(identifier)s, %(truck_identifier)s, %(status)s, %(substatus)s, %(substatus_code)s, %(contact_identifier)s, %(arrived_at)s, %(latitude)s, %(longitude)s, %(bultos)s, %(comuna)s, %(driver)s,
                        %(item_name)s, %(item_quantity)s, %(item_delivered_quantity)s, %(item_code)s);
                """,data)
            self.conn.commit()

    #### beetrack 
    ## Route 
    def insert_beetrack_creacion_ruta(self, data):
            with self.conn.cursor() as cur:
                cur.execute("""
                INSERT INTO beetrack.route
                (resource, evento, account_name, route, account_id, fecha, truck, truck_driver, started, started_at, ended, ended_at)   
                VALUES( %(resource)s, %(event)s, %(account_name)s, %(route)s, %(account_id)s, %(date)s, %(truck)s, %(truck_driver)s, %(started)s,%(started_at)s, %(ended)s, %(ended_at)s);
                """,data)
            self.conn.commit()


    def update_route_beetrack_event(self ,data):
        with self.conn.cursor() as cur:
            cur.execute("""        
            UPDATE beetrack.route
            SET evento = %(event)s, fecha=%(date)s, truck=%(truck)s, truck_driver=%(truck_driver)s, started=%(started)s, started_at=%(started_at)s, ended=%(ended)s, ended_at=%(ended_at)s
            WHERE route = %(route)s;
            """, data)
            row = cur.rowcount
        self.conn.commit()
        return row

    
    ## ruta transyanez
    def insert_beetrack_data_ruta_transyanez(self, data):
            with self.conn.cursor() as cur:
                cur.execute("""
                INSERT INTO beetrack.ruta_transyanez
                (identificador_ruta, identificador, guia, cliente, servicio, region_de_despacho , fecha_estimada, fecha_llegada, estado , usuario_movil, id_cliente, nombre_cliente  , direccion_cliente   , telefono_cliente , correo_electronico_cliente, fechahr    ,email, conductor, fechaentrega, cmn, volumen, bultos, factura, oc, ruta, tienda, is_trunk)
                VALUES( %(route_id)s, %(identifier)s, %(guide)s, %(Cliente)s,  %(Servicio)s, %(Región de despacho)s,  %(estimated_at)s, %(arrived_at)s, %(substatus)s, %(driver)s   ,  %(contact_identifier)s, %(contact_name)s,  %(contact_address)s, %(contact_phone)s, %(contact_email)s         , %(fechahr)s,%(contact_email)s, %(driver)s, %(fechaentrega)s, %(comuna)s, %(volumen)s, %(bultos)s, %(factura)s, %(oc)s, %(ruta)s, %(tienda)s, %(is_trunk)s);

                """,data)
            self.conn.commit()

    #### actualizar ruta transyanez

    def update_ruta_ty_event(self ,data):
        with self.conn.cursor() as cur:
            cur.execute("""        
            UPDATE beetrack.ruta_transyanez
            SET identificador = %(identifier)s,
                cliente = %(Cliente)s,
                servicio = %(Servicio)s,
                region_de_despacho = %(Región de despacho)s,
                fecha_estimada = %(estimated_at)s,
                fecha_llegada = %(arrived_at)s,
                estado = %(substatus)s,
                usuario_movil = %(driver)s,
                id_cliente = %(contact_identifier)s,
                nombre_cliente = %(contact_name)s,
                direccion_cliente = %(contact_address)s,
                telefono_cliente = %(contact_phone)s,
                correo_electronico_cliente = %(contact_email)s,
                fechahr = %(fechahr)s,
                email = %(contact_email)s,
                conductor = %(driver)s,
                fechaentrega = %(fechaentrega)s,
                cmn = %(comuna)s,
                volumen = %(volumen)s,
                bultos = %(bultos)s,
                factura = %(factura)s,
                oc = %(oc)s,
                ruta = %(ruta)s,
                tienda = %(tienda)s,
                is_trunk = %(is_trunk)s
            WHERE guia = %(guide)s AND identificador_ruta = %(route_id)s;
            """, data)
            row = cur.rowcount
        self.conn.commit()
        return row
    

    ### actualizar fotos entrega rutas transyanez

    def update_fotos_entrega_ruta_ty_event(self ,arrays_fotos, guide, route_id):
        with self.conn.cursor() as cur:
            cur.execute(f"""        
            UPDATE beetrack.ruta_transyanez
            SET imagenes = {arrays_fotos}
            WHERE guia = '{guide}' AND identificador_ruta = {route_id}
            """)
            row = cur.rowcount
        self.conn.commit()
        return row
    


    def verificar_si_ruta_existe(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""  
                        select *  from beetrack.ruta_transyanez rt  
                        where coalesce (identificador_ruta, 1) = coalesce(%(ruta_id)s ,1) and  guia = %(guia)s
                        order by  created_at  desc
                 """,data)
            return cur.fetchall()
        
    ##API confirmaFacil Electrolux

    def recuperar_data_electrolux(self):
        with self.conn.cursor() as cur:
            cur.execute("""  
                    select 	--trb.guia,
                            --trb.factura,
                            --trb.estado,
                            coalesce((select ee.cod_elux  
                            from beetrack.estados_electrolux ee
                            where ee.subestado = (select se.code from areati.subestado_entregas se where se.name=trb.estado) limit 1),5) as tipoEntrega,
                            regexp_replace(trb.factura, '[^0-9]', '', 'g') as numero,
                            twce.factura, 
                            coalesce(to_char(trb.fecha_llegada,' dd/mm/yyyy'),to_char(trb.created_at,' dd/mm/yyyy')) as dtOcorrencia,
                            coalesce(trb.fecha_llegada::time,trb.created_at::time) as hrOcorrencia,
                            coalesce(trb.estado,'En Ruta') as comentario
                    from beetrack.ruta_transyanez trb
                    left join areati.ti_wms_carga_electrolux twce on trb.guia = twce.numero_guia
                    where trb.created_at::date = current_date::date
                    and lower(cliente)='electrolux'
                    order by 5 desc
                 """)
            return cur.fetchall()
    
    ### Dashboard pendientes
    def read_lista_pendientes_bodega_hasta_hoy(self):
            with self.conn.cursor() as cur:
                cur.execute("""  
                        select * from areati.pendientes_bodega();
                    """)
                return cur.fetchall()

    ### AREA TI VIKING

    def read_lista_funciones(self):
        with self.conn.cursor() as cur:
            cur.execute("""  
                select * from hades.obtener_info_funciones();
                 """)
            return cur.fetchall()
        
    def read_lista_tipo_funciones(self):
          with self.conn.cursor() as cur:
            cur.execute("""  
                        SELECT id, nombre
                        FROM areati.tipo_funciones;
                    """)
            return cur.fetchall()
          
    def insert_lista_funcion(self, data):
            with self.conn.cursor() as cur:

                parametros_array_str = 'ARRAY[%s]' % (','.join([f"'{res}'" for res in data["arrParametros"]])) if data["arrParametros"] else 'NULL'
                comentario_array_str = 'ARRAY[%s]' % (','.join([f"'{res}'" for res in data["arrComentario"]])) if data["arrComentario"] else 'NULL'
                palabras_clave_array_str = 'ARRAY[%s]' % (','.join([f"'{res}'" for res in data["arrPalabras_clave"]])) if data["arrPalabras_clave"] else 'NULL'
                tablas_impactadas_array_str = 'ARRAY[%s]' % (','.join([f"'{res}'" for res in  data["arrTablas_impactadas"]])) if  data["arrTablas_impactadas"] else 'NULL'

                cur.execute(f"""
                INSERT INTO areati.registro_funciones (esquema, nombre_funcion, tipo_funcion, descripcion, parametros, 
                        comentarios_parametros, palabras_clave, tablas_impactadas) 
                VALUES (
                       '{data["Esquema"]}',
                       '{data["Nombre_funcion"]}',
                        {data["Tipo_funcion"]},
                       '{data["Descripcion"]}',
                        {parametros_array_str},
                        {comentario_array_str},
                        {palabras_clave_array_str},
                        {tablas_impactadas_array_str}
                    );
                """)
                
            self.conn.commit()

    #Y

  ## MANTENEDORES INVENTARIO TI

    def bitacora_asignar_chip(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.bitacora
                        (id_usuario, ids_usuario, id_equipo, id_persona, observacion, 
                        latitud, longitud, id_chip, ubicacionarchivo, estado, subestado)
                        VALUES( %(id_user)s, %(ids_user)s, %(equipo)s, %(persona)s, %(observacion)s, %(lat)s, 
                        %(long)s, %(id_chip)s, %(ubicacionarchivo)s, %(estadoChip)s , %(subestadoChip)s);""", data)
            self.conn.commit()

    def read_chips_asignados_a_equipos(self):
        with self.conn.cursor() as cur:
             cur.execute("""   select a.equipo,  p.nombres || p.apellidos as persona , e.modelo || e.marca as linea, e.serial , e.descripcion as número,
                e.id as id_equipo, e.modelo as celular, e.serial as imei, s.descripcion  as estado
                from inventario.asignacion a  
                inner join inventario.equipo e on a.equipo = e.id 
                inner join inventario.tipo t on e.tipo = t.id
                inner join inventario.persona p on p.id = a.persona 
                inner join inventario.subestados s on a.sub_estado = s.code 
                where a.sub_estado = 4 and e.tipo = 3""")
             return cur.fetchall()
    
    def read_chips_asignados_para_devolucion(self):
        with self.conn.cursor() as cur:
            cur.execute("""   select  b.id_chip, p.id as persona , e.modelo || e.marca as linea, e.serial , e.descripcion as número,
                       e2.id as id_equipo, e2.modelo as celular, e2.serial as imei, s.id as estado
                        from inventario.bitacora b 
                        inner join inventario.equipo e  on b.id_chip = e.id 
                        inner join inventario.equipo e2 on b.id_equipo =e2.id
                        inner join inventario.persona p on b.id_persona  = p.id 
                        inner join inventario.subestados s on e.subestado = s.code
                        where e.subestado = 4""")
            # cur.execute("""                         
            #         select  b.id_chip, p.id as persona , e.modelo || e.marca as linea, e.serial , e.descripcion as número,
            #            e2.id as id_equipo, e2.modelo as celular, e2.serial as imei, s.id as estado
            #             from inventario.bitacora b 
            #             inner join inventario.equipo e  on b.id_chip = e.id 
            #             inner join inventario.equipo e2 on b.id_equipo =e2.id
            #             inner join inventario.persona p on b.id_persona  = p.id 
            #             inner join inventario.subestados s on e.subestado = s.code
            #             where e.subestado = 4
            #                """)
            return cur.fetchall()  
    
    def read_chip_no_asignado(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT e.id, e.marca, e.modelo, e.serial, e.mac_wifi, e.serie, e.resolucion, e.dimensiones, e.descripcion, e.ubicacion,
                        e.almacenamiento, e.ram, es.descripcion AS estado, s.descripcion as subestado, t.nombre AS tipo, e.cantidad, e.nr_equipo 
                        FROM inventario.equipo e
                        INNER JOIN inventario.tipo t ON e.tipo = t.id
                        INNER JOIN inventario.estados es ON e.estado = es.id
                        inner join inventario.subestados s on s.code = e.subestado and s.parent_code = e.estado
                        where e.tipo = 3  and e.subestado = 1 order by e.id DESC ; """)
            return cur.fetchall()
        

    

    def imprimir_planilla_personas(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             SELECT  nombres, apellidos, rut, nacionalidad, fecha_nacimiento, estado_civil, telefono, fecha_ingreso, cargo, domicilio, comuna, banco, 
                        tipo_cuenta, numero_cuenta, correo, afp, salud, telefono_adicional, nombre_contacto, seguro_covid, horario, ceco, sueldo_base, 
                        tipo_contrato, direccion_laboral, enfermedad, polera, pantalon, poleron, zapato
                FROM inventario.persona;
                        """)
            return cur.fetchall()

    def bitacora_inventario_persona(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.bitacora
                        (id_usuario, ids_usuario, id_persona, observacion, latitud, longitud)
                        VALUES( %(id_user)s, %(ids_user)s,  %(id)s, %(observacion)s,  %(lat)s, %(long)s);""", data)
            self.conn.commit()

    def bitacora_inventario_asignacion(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.bitacora
                        (id_usuario, ids_usuario, id_equipo, id_persona, estado, subestado, observacion, 
                         latitud, longitud)
                        VALUES( %(id_usuario)s, %(ids_usuario)s, %(equipo_id)s, %(id)s, %(status)s,
                         %(subestado)s, %(observacion)s, %(lat)s, %(long)s);""", data)
            self.conn.commit()

    def bitacora_inventario_devolucion(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.bitacora
                        (id_usuario, ids_usuario, id_equipo, id_persona, estado, subestado, observacion, 
                         latitud, longitud, ubicacionarchivo)
                        VALUES( %(id_usuario)s, %(ids_usuario)s, %(equipo_id)s, %(id)s, %(status)s,
                         %(subestado)s, %(observacion)s, %(lat)s, %(long)s, %(ubicacionarchivo)s);""", data)
            self.conn.commit()

    def bitacora_accesorios(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.bitacora
                        (id_usuario, ids_usuario, id_equipo, id_persona,  estado, subestado, observacion, latitud, longitud)
                        VALUES( %(id_user)s, %(ids_user)s, %(equipo)s, %(persona)s,  %(status)s,
                         %(subestado)s, %(observacion)s, %(lat)s, %(long)s);""", data)
            self.conn.commit()
    def bitacora_asignar_licencia(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.bitacora
                        (id_usuario, ids_usuario, id_equipo, id_persona, estado, observacion,
                         latitud, longitud, id_licencia, ubicacionarchivo)
                        VALUES( %(id_user)s, %(ids_user)s, %(equipo)s, %(persona)s,  %(status)s, %(observacion)s, %(lat)s,  
                        %(long)s, %(id_licencia)s, %(ubicacionarchivo)s);""", data)
            self.conn.commit()
    
    def bitacora_liberar_licencia(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.bitacora
                        (id_usuario, ids_usuario, id_equipo, id_persona, estado, observacion, 
                         latitud, longitud, id_licencia)
                        VALUES( %(id_user)s, %(ids_user)s, %(equipo)s, %(persona)s , %(status)s, %(observacion)s, %(lat)s, 
                        %(long)s, %(id_chip)s, %(estado)s, %(subestado)s);""", data)
            self.conn.commit()

    def bitacora_liberar_chip(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.bitacora
                        (id_usuario, ids_usuario, id_equipo, id_persona, observacion, 
                         latitud, longitud, id_licencia, estado, subestado)
                        VALUES( %(id_user)s, %(ids_user)s, %(equipo)s, %(persona)s, %(observacion)s, %(lat)s, 
                        %(long)s, %(id_chip)s);""", data)
            self.conn.commit()

    def bitacora_liberar_insumo(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.bitacora
                        (id_usuario, ids_usuario, id_equipo, observacion, 
                         latitud, longitud)
                        VALUES( %(id_user)s, %(ids_user)s, %(id)s, %(observacion)s, %(lat)s, 
                        %(long)s);""", data)
            self.conn.commit()


    def bitacora_inventario_equipo(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.bitacora
                        (id_usuario, ids_usuario, id_equipo, estado, subestado, observacion, 
                        ubicacionarchivo, latitud, longitud)
                        VALUES( %(id_user)s, %(ids_user)s, %(id)s, %(status)s,
                         %(subestado)s, %(observacion)s, %(ubicacionarchivo)s, %(lat)s, %(long)s);""", data)
            self.conn.commit()
    
    def ingresar_personal_equipo(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO inventario.persona(id_user, ids_user, lat, long, nombres, apellidos, rut,
                         nacionalidad, fecha_nacimiento, estado_civil, telefono, fecha_ingreso, cargo, domicilio, 
                        comuna, banco, tipo_cuenta, numero_cuenta, correo, afp, salud, telefono_adicional,
                         nombre_contacto, seguro_covid, horario, ceco, sueldo_base, tipo_contrato, direccion_laboral,
                         enfermedad, polera, pantalon, poleron, zapato, foto,pdf, req_comp, req_cel)
                VALUES(%(id_user)s, %(ids_user)s, %(lat)s, %(long)s, %(nombres)s, %(apellidos)s, %(rut)s, %(nacionalidad)s,
                        %(fecha_nacimiento)s, %(estado_civil)s, %(telefono)s, %(fecha_ingreso)s, %(cargo)s, %(domicilio)s,
                        %(comuna)s, %(banco)s, %(tipo_cuenta)s, %(numero_cuenta)s, %(correo)s, %(afp)s, %(salud)s, %(telefono_adicional)s,
                        %(nombre_contacto)s, %(seguro_covid)s, %(horario)s, %(ceco)s, %(sueldo_base)s, %(tipo_contrato)s, %(direccion_laboral)s,
                        %(enfermedad)s, %(polera)s, %(pantalon)s, %(poleron)s, %(zapato)s, %(foto)s, %(pdf)s, %(req_comp)s, %(req_cel)s)
                       
                        """, data)
            self.conn.commit()

    def ingresar_tipo_equipo(self, data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.tipo (nombre, documentacion)
                        VALUES(%(nombre)s , %(documentacion)s)
            """, data)
        self.conn.commit()

    def ingresar_subestado(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.subestados ( parent_code, code, descripcion)
                VALUES(%(parent_code)s, %(code)s, %(descripcion)s)""",data)
            self.conn.commit()    
    def ingresar_equipo_asignado(self, data):
        with self.conn.cursor() as cur: 
            cur.execute("""INSERT INTO inventario.asignacion
                (id_user, ids_user, lat, long, equipo, persona, fecha_entrega, estado,
                         fecha_devolucion, observacion, folio_entrega, folio_devolucion, firma_entrega, firma_devolucion,
                        pdf_entrega, pdf_devolucion,departamento) 
                VALUES(%(id_user)s, %(ids_user)s,%(lat)s,%(long)s,%(equipo)s,%(persona)s,%(fecha_entrega)s,
                %(estado)s,%(fecha_devolucion)s,%(observacion)s,%(folio_entrega)s,%(folio_devolucion)s, %(firma_entrega)s
                        ,%(firma_devolucion)s, %(pdf_entrega)s,%(pdf_devolucion)s, %(departamento)s)""",data)
        self.conn.commit()

    def ingresar_chip_asignado(self, data):
        with self.conn.cursor() as cur: 
            cur.execute("""INSERT INTO inventario.asignacion
                (id_user, ids_user, lat, long, equipo, persona, fecha_entrega, estado,
                         fecha_devolucion, observacion,folio_entrega, folio_devolucion, departamento, sub_estado) 
                VALUES(%(id_user)s, %(ids_user)s,%(lat)s,%(long)s,%(id_chip)s,%(persona)s,%(fecha_entrega)s,
                %(estado)s,%(fecha_devolucion)s,%(observacion)s, %(folio_entrega)s,%(folio_devolucion)s, %(departamento)s, %(subestadoChip)s)""",data)
        self.conn.commit()

    def ingresar_accesorio_asignado(self, data):
        with self.conn.cursor() as cur: 
            cur.execute("""INSERT INTO inventario.asignacion
                 (id_user, ids_user, lat, long, equipo, persona, fecha_entrega, estado, observacion,departamento, sub_estado,
                 folio_entrega, folio_devolucion) 
                 VALUES(%(id_user)s, %(ids_user)s,%(lat)s,%(long)s,%(equipo)s, %(persona)s, %(fecha_entrega)s,
                %(estado)s,%(observacion)s, %(departamento)s, %(sub_estado)s, %(folio_entrega)s, %(folio_devolucion)s)""",data)
        self.conn.commit()

    def devolucion_accesorio_asignado(self, data):
        with self.conn.cursor() as cur: 
            cur.execute("""INSERT INTO inventario.asignacion
                (id_user, ids_user, lat, long, equipo,  fecha_entrega, estado, observacion,departamento, sub_estado) 
                VALUES(%(id_user)s, %(ids_user)s,%(lat)s,%(long)s,%(equipo)s,%(fecha_entrega)s,
                %(estado)s,%(observacion)s, %(departamento)s, %(sub_estado)s)""",data)
        self.conn.commit()

    def agregar_descripcion_equipo(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.equipo (id_user, ids_user, lat, long, marca, modelo, serial, mac_wifi, serie, resolucion,
                         dimensiones, descripcion, ubicacion, almacenamiento, ram, estado, tipo, cantidad, nr_equipo, subestado, ubicacionarchivo)
                        VALUES(%(id_user)s,%(ids_user)s,%(lat)s,%(long)s,%(marca)s,%(modelo)s,%(serial)s,%(mac_wifi)s,%(serie)s,%(resolucion)s,
                        %(dimensiones)s,%(descripcion)s,%(ubicacion)s,%(almacenamiento)s,%(ram)s,%(estado)s,%(tipo)s, %(cantidad)s, %(nr_equipo)s,
                        %(subestado)s, %(ubicacionarchivo)s)
                        """,data)
        self.conn.commit()  

    def asignar_equipo_personal(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.asignacion
                        (id_user, ids_user, lat, long, equipo, persona, fecha_entrega, estado, fecha_devolucion, observacion,
                         folio_entrega, firma_entrega, departamento)
                        VALUES(%(id_user)s,%(ids_user)s,%(lat)s,%(long)s,%(equipo)s,%(persona)s,%(fecha_entrega)s,%(estado)s,%(fecha_devolucion)s,
                        %(observacion)s,%(folio_entrega)s, %(firma_entrega)s, %(departamento)s)
                        """,data)
        self.conn.commit()
    def ingresar_departamento(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.departamento
                    ( id_user, ids_user, nombre)
                    VALUES (%(id_user)s, %(ids_user)s, %(nombre)s )""", data)
        self.conn.commit()

    def ingresar_estado(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.estados
            (estado, descripcion) VALUES(%(estado)s, %(descripcion)s ) """, data)
        self.conn.commit()
    
    def ingresar_licencia(self, data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.licencias
                ( id_user, ids_user, lat, long, codigo, asignada) VALUES(%(id_user)s, %(ids_user)s,%(lat)s,%(long)s,%(codigo)s ,%(asignada)s) """,data)
        self.conn.commit()

    def ingresar_sucursal(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.sucursales
                    (nombre, pais, ciudad, comuna, direccion, latitud, longitud, id_usuario, ids_usuario)
                        VALUES (%(nombre)s, %(pais)s, %(ciudad)s, %(comuna)s, %(direccion)s, %(latitud)s, 
                        %(longitud)s, %(id_usuario)s, %(ids_usuario)s)""",data)
        self.conn.commit()
        
    def read_personas(self):
        with self.conn.cursor() as cur:
            cur.execute(""" Select id, nombres, apellidos, rut, nacionalidad, fecha_nacimiento, estado_civil, telefono,
                         fecha_ingreso, cargo, domicilio, comuna, banco, tipo_cuenta, numero_cuenta, correo, afp, salud,
                         telefono_adicional, nombre_contacto, seguro_covid, horario, ceco, sueldo_base, tipo_contrato,
                         direccion_laboral, enfermedad, polera, pantalon, poleron, zapato, foto, pdf, req_comp, req_cel, habilitado
                    FROM inventario.persona order by nombres asc;
                """)
            return cur.fetchall()
    def read_personas_habilitadas(self):
        with self.conn.cursor() as cur:
            cur.execute(""" Select id, nombres, apellidos, rut, nacionalidad, fecha_nacimiento, estado_civil, telefono,
                         fecha_ingreso, cargo, domicilio, comuna, banco, tipo_cuenta, numero_cuenta, correo, afp, salud,
                         telefono_adicional, nombre_contacto, seguro_covid, horario, ceco, sueldo_base, tipo_contrato,
                         direccion_laboral, enfermedad, polera, pantalon, poleron, zapato, foto, pdf, req_comp, req_cel, habilitado
                    FROM inventario.persona where habilitado= true order by nombres asc;
                """)
            return cur.fetchall()
        
    def read_ultima_persona(self):
        with self.conn.cursor() as cur:
            cur.execute(""" Select id
                    FROM inventario.persona order by id desc limit 1 """)
            return cur.fetchone()
  
    def read_ultimo_equipo(self):
        with self.conn.cursor() as cur:
            cur.execute(""" Select id, estado, subestado
                    FROM inventario.equipo order by id desc limit 1 """)
            return cur.fetchone()
        
    def read_persona_por_rut(self,rut):
        with self.conn.cursor() as cur:
            cur.execute("""  Select id, nombres, apellidos, rut, nacionalidad, fecha_nacimiento, estado_civil, telefono,
                         fecha_ingreso, cargo, domicilio, comuna, banco, tipo_cuenta, numero_cuenta, correo, afp, salud,
                         telefono_adicional, nombre_contacto, seguro_covid, horario, ceco, sueldo_base, tipo_contrato,
                         direccion_laboral, enfermedad, polera, pantalon, poleron, zapato, foto, pdf,  req_comp, req_cel,habilitado
                    FROM inventario.persona where rut=%(rut)s""", {"rut" : rut} )
            return cur.fetchall()
        
    
        
    def read_licencia_windows(self):
        with self.conn.cursor() as cur:
             cur.execute(""" SELECT id,  codigo
                    FROM inventario.licencias; """)
             return cur.fetchall()
        
    def read_licencia_asignadas(self):
        with self.conn.cursor() as cur:
             cur.execute(""" select b.id_licencia, l.codigo from inventario.bitacora b 
                            inner join inventario.licencias l on b.id_licencia = l.id where l.asignada = true  """)
             return cur.fetchall()
            
    def read_licencias_no_asignadas(self):
         with self.conn.cursor() as cur:
             cur.execute("""  SELECT l.id AS id_licencia, l.codigo
                            FROM inventario.licencias l
                            WHERE  l.asignada  = false;  """)
             return cur.fetchall()
         
    
    ## posible bug, para obtener los datos de a quien fue entregada de la licencia se utiliza la bitacora, donde se repiten varios datos por lo cual 
    ## es posible que al mostrar la lista muestre datos erroneos, se realiza el distinc para solo mostrar un codigo de licencia por vez y se orden por fecha
    ## para mostrar la ultima asignacion de esa licencia
         
    def read_licencias_asignadas_a_equipos(self):
        with self.conn.cursor() as cur:
             # cur.execute(""" select l.id, p.nombres || p.apellidos as persona , e.modelo || e.marca as equipo, e.serial , l.codigo,
            #             l.asignada as asignada, e.id as id_equipo, p.id as id_persona
            #             from inventario.bitacora b 
            #             inner join inventario.licencias l on b.id_licencia = l.id 
            #             inner join inventario.equipo e on b.id_equipo = e.id
            #             inner join inventario.persona p on b.id_persona  = p.id 
            #             where l.asignada = true and b.estado = 1""")
            cur.execute(""" select DISTINCT ON (l.codigo) l.id, p.nombres || p.apellidos as persona , e.modelo || e.marca as equipo, e.serial , l.codigo,
                        l.asignada as asignada, e.id as id_equipo, p.id as id_persona, b.id as bitacora_id
                        from inventario.bitacora b 
                        inner join inventario.licencias l on b.id_licencia = l.id 
                        inner join inventario.equipo e on b.id_equipo = e.id
                        inner join inventario.persona p on b.id_persona  = p.id 
                        where l.asignada = true order by  l.codigo,b.created_at desc""")
            return cur.fetchall()
        
    def read_tipo_equipo(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT id, nombre FROM inventario.tipo; """)
            return cur.fetchall()
        
    def read_tipo_con_documentacion(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT id, nombre FROM inventario.tipo where documentacion = true; """)
            return cur.fetchall()
        
    def read_tipo_sin_documentacion(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT id, nombre FROM inventario.tipo where documentacion = false; """)
            return cur.fetchall()
        
    def read_estado(self):
        with self.conn.cursor() as cur:
            cur.execute(""" select * from inventario.estados""")
            return cur.fetchall()
        
    def read_ultimo_estado(self):
        with self.conn.cursor()as cur:
            cur.execute(""" select MAX(estado) from inventario.estados """)     
            return cur.fetchall()

    def read_descripcion_equipo(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT e.id, e.marca, e.modelo, e.serial, e.mac_wifi, e.serie, e.resolucion, e.dimensiones, e.descripcion, e.ubicacion,
                        e.almacenamiento, e.ram, es.descripcion AS estado, s.descripcion as subestado, t.nombre AS tipo, e.cantidad, e.nr_equipo 
                        FROM inventario.equipo e
                        INNER JOIN inventario.tipo t ON e.tipo = t.id
                        INNER JOIN inventario.estados es ON e.estado = es.id
                        inner join inventario.subestados s on s.code = e.subestado and s.parent_code = e.estado order by e.id DESC ; """)
            return cur.fetchall()
        
    def read_descripcion_por_id(self,id):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT e.id, e.marca, e.modelo, e.serial, e.mac_wifi, e.serie, e.resolucion, e.dimensiones, e.descripcion, e.ubicacion,
                        e.almacenamiento, e.ram, es.descripcion AS estado, s.descripcion as subestado, t.nombre AS tipo, e.cantidad, e.nr_equipo 
                        FROM inventario.equipo e
                        INNER JOIN inventario.tipo t ON e.tipo = t.id
                        INNER JOIN inventario.estados es ON e.estado = es.id
                        inner join inventario.subestados s on s.code = e.subestado and s.parent_code = e.estado where e.id=%(id)s  """, {"id" : id})
            return cur.fetchall()
        
    def read_lista_equipos(self):
        with self.conn.cursor() as cur:
            cur.execute("""SELECT id, marca, modelo, serial, mac_wifi, serie, resolucion, dimensiones, descripcion, ubicacion, almacenamiento, ram, estado, tipo,
                         cantidad, nr_equipo, subestado
                    FROM inventario.equipo; """)
            return cur.fetchall()
        
    def read_equipo_disponible(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT e.id, e.marca, e.modelo, e.serial, e.mac_wifi, e.serie, e.resolucion, e.dimensiones, e.descripcion, e.ubicacion,
                        e.almacenamiento, e.ram, es.descripcion AS estado, s.descripcion as subestado, t.nombre AS tipo, e.cantidad, e.nr_equipo 
                        FROM inventario.equipo e
                        INNER JOIN inventario.tipo t ON e.tipo = t.id
                        INNER JOIN inventario.estados es ON e.estado = es.id
                        inner join inventario.subestados s on s.code = e.subestado and s.parent_code = e.estado where e.estado != 1 and e.estado != 3 and e.subestado !=3 ; """)
            return cur.fetchall()
        
    def read_equipo_por_serial(self, serial):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT e.id, e.marca, e.modelo, e.serial, e.mac_wifi, e.serie, e.resolucion, e.dimensiones, e.descripcion, e.ubicacion,
                        e.almacenamiento, e.ram, es.descripcion AS estado, s.descripcion as subestado, t.nombre AS tipo, e.cantidad, e.nr_equipo 
                        FROM inventario.equipo e
                        INNER JOIN inventario.tipo t ON e.tipo = t.id
                        INNER JOIN inventario.estados es ON e.estado = es.id
                        inner join inventario.subestados s on s.code = e.subestado and s.parent_code = e.estado  where e.serial=%(serial)s """, {"serial" : serial})
            return cur.fetchall()
        
    def read_todos_los_insumos_asignados_por_serial(self,serial):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT e.id, d.nombre as departamento,t.nombre AS tipo, e.marca, e.modelo, e.serial, es.descripcion AS estado, s.descripcion as subestado,
                            a.fecha_entrega, a.fecha_devolucion 
                        FROM inventario.equipo e
                        inner join inventario.asignacion a on a.equipo  =e.id
                        inner join inventario.departamento d on d.id =a.departamento
                        INNER JOIN inventario.tipo t ON e.tipo = t.id
                        INNER JOIN inventario.estados es ON e.estado = es.id
                        inner join inventario.subestados s on s.code = e.subestado   and s.parent_code = e.estado 
                         where e.serial=%(serial)s """, {"serial":serial})
            return cur.fetchall()

    def read_equipos_general(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT id, marca, modelo, serial, mac_wifi, serie, resolucion, dimensiones,
                         descripcion, ubicacion, almacenamiento, ram, estado, subestado, tipo, cantidad, nr_equipo
                        FROM inventario.equipo; """)
            return cur.fetchall()
    def read_equipos_by_tipo(self, tipo):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT id, marca, modelo, serial, mac_wifi, serie, resolucion, dimensiones,
                         descripcion, ubicacion, almacenamiento, ram, estado, tipo, cantidad, nr_equipo, subestado
                        FROM inventario.equipo where tipo=%(tipo)s and  estado != 1 and estado != 3 and subestado != 3""", {"tipo" : tipo})
            return cur.fetchall()
        

    def read_equipos_by_persona_chip(self, id):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT e.id, e.marca, e.modelo, e.serial, e.mac_wifi, e.serie, e.resolucion, e.dimensiones,
                         e.descripcion, e.ubicacion, e.almacenamiento, e.ram, e.estado, e.tipo, e.cantidad, e.nr_equipo,
                         e.subestado
                        FROM inventario.equipo e
                        inner join inventario.asignacion a on a.equipo  = e.id 
                        where e.tipo=2 and a.persona =%(id)s and e.estado = 1""", {"id" : id})
            return cur.fetchall()

    def read_equipos_by_persona_notebook(self, id):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT e.id, e.marca, e.modelo, e.serial, e.mac_wifi, e.serie, e.resolucion, e.dimensiones,
                         e.descripcion, e.ubicacion, e.almacenamiento, e.ram, e.estado, e.tipo, e.cantidad, e.nr_equipo,
                         e.subestado
                        FROM inventario.equipo e
                        inner join inventario.asignacion a on a.equipo  = e.id 
                        where e.tipo=1 and a.persona =%(id)s and e.estado = 1""", {"id" : id})
            return cur.fetchall()

    def read_persona_by_departamento(self, departamento):
        with self.conn.cursor() as cur:
            cur.execute(""" Select id, nombres, apellidos, rut, nacionalidad, fecha_nacimiento, estado_civil, telefono,
                         fecha_ingreso, cargo, domicilio, comuna, banco, tipo_cuenta, numero_cuenta, correo, afp, salud,
                         telefono_adicional, nombre_contacto, seguro_covid, horario, ceco, sueldo_base, tipo_contrato,
                         direccion_laboral, enfermedad, polera, pantalon, poleron, zapato, foto, pdf, req_comp, req_cel, habilitado
                    FROM inventario.persona where habilitado= true and departamento=%(departamento)s order by nombres asc""", {"departamento" : departamento})
            return cur.fetchall()
    
    
    def read_asignaciones_personal(self):
        with self.conn.cursor() as cur:
            cur.execute("""SELECT a.id, p.nombres || ' ' || p.apellidos  as persona, d.nombre as departamento,
                        e.marca|| ' ' || e.modelo AS equipo,  a.folio_entrega,  a.fecha_entrega,a.firma_entrega, a.pdf_entrega ,a.folio_devolucion,  a.fecha_devolucion,
                        a.firma_devolucion ,a.pdf_devolucion , a.estado, a.observacion, t.nombre as tipo
                    FROM inventario.asignacion a  
                        INNER JOIN inventario.persona p ON a.persona = p.id
                        INNER JOIN inventario.equipo e ON a.equipo = e.id
                        INNER JOIN inventario.departamento d ON a.departamento = d.id
                        inner join inventario.tipo t on e.tipo = t.id where t.documentacion = true
                        order by nombres asc """)
            return cur.fetchall()
        
    def read_accesorios_asignados(self):
        with self.conn.cursor() as cur:
            cur.execute("""SELECT e.id, d.nombre as departamento, e.marca|| ' ' || e.modelo AS equipo,  p.nombres ||' ' || p.apellidos  as persona, a.fecha_entrega,
                         a.estado, a.observacion, a.id as id_asignacion
                        FROM inventario.asignacion a  
                        INNER JOIN inventario.equipo e ON a.equipo = e.id
                        INNER JOIN inventario.departamento d ON a.departamento = d.id
                        inner join inventario.tipo t on e.tipo = t.id
                        inner join inventario.persona p on p.id = a.persona 
                        where t.documentacion = false  """)
            return cur.fetchall()
        
    def read_insumos_asignados(self):
        with self.conn.cursor() as cur:
            cur.execute("""                     
                         SELECT e.id as id , d.nombre as departamento, e.marca|| ' ' || e.modelo AS equipo,  a.fecha_entrega,
                         a.estado, a.observacion, a.id as id_asignacion
                        FROM inventario.asignacion a  
                        INNER JOIN inventario.equipo e ON a.equipo = e.id
                        INNER JOIN inventario.departamento d ON a.departamento = d.id
                        inner join inventario.tipo t on e.tipo = t.id
                        where  a.estado =true  and e.tipo = 7 or e.tipo = 15   """)
            return cur.fetchall()
    
    def read_asignados_sin_join(self):
        with self.conn.cursor() as cur:
            cur.execute(""" 
                        SELECT a.id,   a.persona, a.departamento, a.equipo, a.folio_entrega, a.fecha_entrega,
                        a.firma_entrega, a.pdf_entrega, a.folio_devolucion,
                        a.fecha_devolucion, a.firma_devolucion, a.pdf_devolucion,  a.estado, a.observacion,t.nombre
                        from inventario.asignacion a
                        INNER JOIN inventario.equipo e ON a.equipo = e.id
                        inner join inventario.tipo t on e.tipo = t.id
                     
                 """)
            return cur.fetchall()
    def read_asignados_sin_join_por_id(self,id):
        with self.conn.cursor() as cur:
            cur.execute("""   SELECT a.id,   a.persona, a.departamento, a.equipo, a.folio_entrega, a.fecha_entrega,
                        a.firma_entrega, a.pdf_entrega, a.folio_devolucion,
                        a.fecha_devolucion, a.firma_devolucion, a.pdf_devolucion,  a.estado, a.observacion,t.nombre
                        from inventario.asignacion a
                        INNER JOIN inventario.equipo e ON a.equipo = e.id
                        inner join inventario.tipo t on e.tipo = t.id
                where a.id=%(id)s """, {"id" : id})
            return cur.fetchall()

    def read_asignados_por_id(self,id):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT a.id, p.nombres, p.apellidos, p.rut , p.cargo , d.nombre as departamento,
                        e.marca, e.serial ,e.marca|| ' ' || e.modelo AS equipo,  a.folio_entrega, a.fecha_entrega 
                        ,a.estado, e.id as equipo_id, a.folio_devolucion, e.descripcion, e.almacenamiento, e.ram, t.nombre AS tipo,
                        a.firma_entrega, a.firma_devolucion, a.pdf_entrega, a.pdf_devolucion
                    FROM inventario.asignacion a   
                        INNER JOIN inventario.persona p ON a.persona = p.id
                        INNER JOIN inventario.equipo e ON a.equipo = e.id
                        INNER JOIN inventario.departamento d ON a.departamento = d.id
                        inner join inventario.tipo t on e.tipo = t.id where a.id=%(id)s """, {"id" : id})
          
            return cur.fetchall()
        
    def read_devolucion_por_id(self,id):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT a.id, p.nombres, p.apellidos, p.rut , p.cargo , d.nombre as departamento,
                        e.marca, e.serial ,e.marca|| ' ' || e.modelo AS equipo,  a.folio_devolucion,  a.fecha_devolucion
                        ,a.estado, e.id as equipo_id
                    FROM inventario.asignacion a  
                        INNER JOIN inventario.persona p ON a.persona = p.id
                        INNER JOIN inventario.equipo e ON a.equipo = e.id
                        INNER JOIN inventario.departamento d ON a.departamento = d.id
                        inner join inventario.tipo t on e.tipo = t.id where a.id=%(id)s """, {"id" : id})
            return cur.fetchall()
        
    def read_equipos_por_persona(self,persona):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT a.id, p.nombres, p.apellidos, p.rut, p.cargo, d.nombre as departamento,e.marca, e.serial ,
                   e.marca || ' ' || e.modelo AS equipo, a.folio_entrega, a.fecha_entrega, e.descripcion, e.almacenamiento,
                   e.ram,e.id AS equipo_id, t.nombre AS tipo, a.estado 
                    FROM inventario.asignacion a  
                INNER JOIN inventario.persona p ON a.persona = p.id
                INNER JOIN inventario.equipo e ON a.equipo = e.id
                INNER JOIN inventario.departamento d ON a.departamento = d.id
                INNER JOIN inventario.tipo t ON e.tipo = t.id
                WHERE p.rut =%(persona)s and a.estado = false and a.firma_devolucion =false""", {"persona": persona})
            return cur.fetchall()
        
    def read_todos_los_equipos_asignados_por_persona(self,persona):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT a.id, p.nombres || ' ' || p.apellidos as persona , p.rut, p.cargo, d.nombre as departamento,e.marca, e.serial ,
                   e.marca || ' ' || e.modelo AS equipo, a.fecha_entrega, e.descripcion, e.id AS equipo_id, t.nombre AS tipo, a.estado 
                    FROM inventario.asignacion a  
                INNER JOIN inventario.persona p ON a.persona = p.id
                INNER JOIN inventario.equipo e ON a.equipo = e.id
                INNER JOIN inventario.departamento d ON a.departamento = d.id
                INNER JOIN inventario.tipo t ON e.tipo = t.id
                WHERE p.rut =%(persona)s and a.estado = true""", {"persona": persona})
            return cur.fetchall()

    def read_todos_los_equipos_asignados_por_serial(self,serial):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT e.id, p.nombres || ' ' || p.apellidos as persona,t.nombre AS tipo, e.marca, e.modelo, e.serial, es.descripcion AS estado, s.descripcion as subestado,
                            a.fecha_entrega, a.fecha_devolucion 
                        FROM inventario.equipo e
                        inner join inventario.asignacion a on a.equipo  =e.id
                        inner join inventario.persona p on p.id =a.persona 
                        INNER JOIN inventario.tipo t ON e.tipo = t.id
                        INNER JOIN inventario.estados es ON e.estado = es.id
                        inner join inventario.subestados s on s.code = e.subestado   and s.parent_code = e.estado 
                         where e.serial=%(serial)s """, {"serial":serial})
            return cur.fetchall()
        
    def read_equipos_por_persona_por_devolver(self,persona):
          with self.conn.cursor() as cur:
            cur.execute(""" SELECT a.id, p.nombres, p.apellidos, p.rut, p.cargo, d.nombre as departamento,e.marca, e.serial ,
                   e.marca || ' ' || e.modelo AS equipo, a.folio_entrega, a.fecha_entrega, e.descripcion, e.almacenamiento, e.ram,
                    e.id AS equipo_id, t.nombre AS tipo, a.estado
                    FROM inventario.asignacion a  
                INNER JOIN inventario.persona p ON a.persona = p.id
                INNER JOIN inventario.equipo e ON a.equipo = e.id
                INNER JOIN inventario.departamento d ON a.departamento = d.id
                INNER JOIN inventario.tipo t ON e.tipo = t.id
                WHERE p.rut =%(persona)s and a.estado = true  and (e.tipo = 1 or e.tipo  = 2 or e.tipo  = 14)""", {"persona": persona})
            return cur.fetchall()
          
    def read_equipos_devueltos_por_persona(self,persona):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT a.id, p.nombres, p.apellidos, p.rut, p.cargo, d.nombre as departamento,e.marca, e.serial ,
                   e.marca || ' ' || e.modelo AS equipo, a.folio_entrega, a.fecha_entrega,
                   a.estado, e.id AS equipo_id
                    FROM inventario.asignacion a  
                INNER JOIN inventario.persona p ON a.persona = p.id
                INNER JOIN inventario.equipo e ON a.equipo = e.id
                INNER JOIN inventario.departamento d ON a.departamento = d.id
                INNER JOIN inventario.tipo t ON e.tipo = t.id
                WHERE p.rut =%(persona)s and a.estado = false  and e.estado = 4""", {"persona": persona})
            return cur.fetchall()
    def read_sucursales_ti(self):
        with self.conn.cursor() as cur: 
            cur.execute(""" SELECT id, nombre, pais, ciudad, comuna, direccion 
                    FROM inventario.sucursales; """)
            return cur.fetchall()
        
    def read_folio_entrega(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT folio_entrega from inventario.asignacion order by folio_entrega DESC LIMIT  1 """)
            resultado = cur.fetchone()
            if resultado and resultado[0] is not None:
                ultimo_folio = resultado[0] +1 
            else:
                ultimo_folio = 1
            return ultimo_folio 
    
    def read_folio_devolucion(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT folio_devolucion from inventario.asignacion order by folio_devolucion DESC LIMIT  1 """)
            resultado = cur.fetchone()
            if resultado and resultado[0] is not None:
                ultimo_folio =  resultado[0] +1  
            else:
                ultimo_folio = 1
            return ultimo_folio 

    def read_nr_equipo(self, tipo):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT MAX(nr_equipo)  from inventario.equipo where tipo=%(tipo)s""",{"tipo": tipo})
            resultado = cur.fetchone()
            if resultado and resultado[0] is not None:
                ultimo_nr = int(resultado[0]) + 1
            else:
                ultimo_nr = 1
        
        return ultimo_nr
    
    def read_nr_code(self, parent_code):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT MAX(code) from inventario.subestados where parent_code=%(parent_code)s""",  {"parent_code": parent_code})
            resultado = cur.fetchone()
            if resultado and resultado[0] is not None:
                ultimo_nr = int(resultado[0]) +1
            else:
                ultimo_nr = 1
        return ultimo_nr

        
    def read_departamento_inventario(self):
        with self.conn.cursor() as cur:
            cur.execute("""SELECT id, nombre
                FROM inventario.departamento; """)
            return cur.fetchall()
        
    def read_estado_inventario(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT id, nombre
                        FROM inventario.estado;""")
            return cur.fetchall()
        
    def read_estados_devolucion(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT * from inventario.estados where estado != 4 and estado != 1""")
            return cur.fetchall()
        
    def read_estado_chip(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT * from inventario.estados where estado = 4""")
            return cur.fetchall()
        
    def read_subestado(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT * FROM inventario.subestados """)
            return cur.fetchall()
        
    def read_chip_by_estado(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT e.id, e.marca, e.modelo, e.serial, e.mac_wifi, e.serie, e.resolucion, e.dimensiones, e.descripcion, e.ubicacion,
                        e.almacenamiento, e.ram, es.descripcion AS estado, s.descripcion as subestado, t.nombre AS tipo, e.cantidad, e.nr_equipo 
                        FROM inventario.equipo e
                        INNER JOIN inventario.tipo t ON e.tipo = t.id
                        INNER JOIN inventario.estados es ON e.estado = es.id
                        inner join inventario.subestados s on s.code = e.subestado and s.parent_code = e.estado
                        where e.tipo = 3  order by e.id DESC ; """)
            return cur.fetchall()
        
    def read_subestado_chip(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT * FROM inventario.subestados where parent_code = 4""")
            return cur.fetchall()
        
    def read_subestado_por_id(self, parent_code):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT * FROM inventario.subestados where parent_code=%(parent_code)s """ ,  {"parent_code": parent_code})
            return cur.fetchall()
        

    def read_estados_devolver(self):
        with self.conn.cursor() as cur:
            cur.execute(""" select id,nombre from inventario.estado e where id != 1 and id != 2 and id != 3 and id != 4 and id != 11 and id != 12 and id != 13 and id !=14 and id != 10 """)
            return cur.fetchall()
        
    def encontrar_por_folio(self, folio):
        with self.conn.cursor() as cur:
            cur.execute(f"""SELECT p.nombres || ' ' || p.apellidos  as persona, d.nombre as departamento, a.nombre_equipo , e.marca|| ' ' || e.modelo as equipo ,  a.folio , a.fecha_entrega,  a.fecha_devolucion , a.estado, a.observacion
                        FROM inventario.asignacion a
                        INNER JOIN inventario.persona p ON a.persona = p.id
                        INNER JOIN inventario.equipo e ON a.equipo = e.id
                        INNER JOIN inventario.departamento d ON a.departamento = d.id
                        inner join inventario.tipo t on e.tipo = t.id where a.folio= %s""", (folio,))
            return cur.fetchall()
        
    def read_estados_entregas(self,id):
        with self.conn.cursor () as cur: 
            cur.execute("""SELECT id, equipo, persona, fecha_entrega, estado, fecha_devolucion, observacion, folio_entrega, folio_devolucion, firma_entrega, firma_devolucion, pdf_entrega, pdf_devolucion, departamento
                FROM inventario.asignacion where id=%(id)s """, {"id" : id})
            return cur.fetchall()
        
    def read_firma_devolucion(self,id):
        with self.conn.cursor () as cur:
            cur.execute("""SELECT id,firma_devolucion, pdf_devolucion from inventario.asignacion a where id=%(id)s """, {"id" : id})
            return cur.fetchall()
        
    def read_firma_entrega(self,id):
        with self.conn.cursor () as cur:
            cur.execute("""SELECT id, firma_entrega, pdf_entrega from inventario.asignacion a where id=%(id)s """, {"id" : id})
            return cur.fetchall()
     ## devolucion de equipo    
    
    def asignar_devolucion_equipo(self, data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.asignacion  
                        SET estado=%(estado)s, fecha_devolucion=%(fecha_devolucion)s, observacion=%(observacion)s
                        WHERE folio=%(folio)s""", data)


        self.conn.commit()

    def asignar_devolucion_accesorio(self, data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.asignacion  
                        SET estado=%(estado)s, fecha_devolucion=%(fecha_devolucion)s, observacion=%(observacion)s
                        WHERE id=%(id)s""", data)


        self.conn.commit()

    ## se actualiza tabla de licencias para liberar la licencia seleccionada y poder reasignarla
        
    def liberar_licencia(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.licencias SET asignada=%(asignado)s where id=%(id_licencia)s""",data)
            self.conn.commit()
    

    def liberar_chip(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.equipo SET estado=%(estadoChip)s ,subestado=%(subestadoChip)s where id=%(id_chip)s""",data)
            self.conn.commit()

    def devolver_chip(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.asignacion SET fecha_devolucion=%(fecha_devolucion)s,  sub_estado=%(subestadoChip)s, estado=%(estado)s where id=%(id_chip)s""",data)
            self.conn.commit()

    def liberar_insumo(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.equipo SET estado=%(estadoInsumo)s ,subestado=%(subestadoInsumo)s where id=%(equipo)s""",data)
            self.conn.commit()

    def devolver_insumo(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.asignacion SET fecha_devolucion=%(fecha_devolucion)s, folio_devolucion=%(folio_devolucion)s, sub_estado=%(subestadoInsumo)s, estado=%(status)s  where id=%(id)s""",data)
            self.conn.commit()



    ##INGRESAR RUTAS DE PDF

    ##se envia la ruta del pdf sin el "/" para que al momento de descargarlo se encuentre la ruta
    def update_campo_pdf_entrega(self,folio_entrega, id):
        with self.conn.cursor() as cur:
            cur.execute(f""" UPDATE inventario.asignacion 
                        SET pdf_entrega='pdfs/acta_entrega/{folio_entrega}.pdf' where id={id}""")
            self.conn.commit()

    def update_campo_pdf_devolucion(self,folio_devolucion, id):
        with self.conn.cursor() as cur:
            cur.execute(f""" UPDATE inventario.asignacion 
                        SET pdf_devolucion='pdfs/acta_devolucion/{folio_devolucion}.pdf' where id={id}""")
            self.conn.commit()

    ## pdf scaneado con la firma de entrega o devolucion
    def upload_pdf_entrega(self, data,id):
        with self.conn.cursor() as cur:
            cur.execute(f""" UPDATE inventario.asignacion set pdf_entrega='pdfs/acta_entrega_firma/{data}' where id={id} """)
            self.conn.commit()

    ##ubicar ruta de pdf

    def read_ubicacion_pdf_devolucion(self,id):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT pdf_devolucion from inventario.asignacion where id=%(id)s""", {"id" : id})
            return cur.fetchall()

    def read_ubicacion_pdf_entrega(self,id):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT pdf_entrega from inventario.asignacion where id=%(id)s""", {"id" : id})
            return cur.fetchone()


    ##EDITAR TABLAS  DE INVENTARIO

    def editar_tipo_equipo(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.tipo
                        SET nombre=%(nombre)s, documentacion=%(documentacion)s where id=%(id)s""", data)
        self.conn.commit()

    def editar_departamento(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.departamento
                        SET nombre=%(nombre)s where id=%(id)s""",data )
        self.conn.commit()

    def editar_licencia(self,data):
        with self.conn.cursor() as cur: 
            cur.execute(""" UPDATE inventario.licencias
                        SET codigo=%(codigo)s where id=%(id)s """, data)
        self.conn.commit()

    def editar_sucursal(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""UPDATE inventario.sucursales 
                        SET nombre=%(nombre)s, pais=%(pais)s, ciudad=%(ciudad)s, comuna=%(comuna)s,direccion=%(direccion)s
                        where id=%(id)s """,data)
        self.conn.commit()

    def editar_estado(self, data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.estado  SET nombre=%(nombre)s where id=%(id)s """,data)
        self.conn.commit()

    def editar_subestado(self, data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.subestados  SET descripcion=%(descripcion)s where id=%(id)s """,data)
        self.conn.commit()
    ## al crear la asignacion del equipo se realiza el cambio del estado en la tabla de equipo para que este no pueda ser
    #elegido nuevamente por error
        
    def actualizar_estado_equipo(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.equipo SET estado=%(status)s, ubicacionarchivo=%(ubicacionarchivo)s where id=%(equipo)s""", data)
        self.conn.commit()


    ##al asignar un chip se le cambia el estado a asignado
    def actualizar_estado_chip(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.equipo SET estado=%(estadoChip)s, subestado=%(subestadoChip)s  where id=%(id_chip)s""", data)
        self.conn.commit()

    ## la tabla de licencia tiene un campo para distinguir cuando esta asignado o libre para usar
    def actualizar_estado_licencia(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.licencias SET asignada=%(asignado)s  where id=%(id_licencia)s""", data)
        self.conn.commit()
        
    ##cambio de estado del equipo al asignar reciente (cambia a estado POR ENTREGAR)

    def actualizar_por_entregar(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.equipo SET ubicacion=%(ubicacion)s, estado=%(status)s where id=%(id)s """, data) 
        self.conn.commit()  
    
    ##cambio de estado al equipo como entregado una vez realiza la firma del acta de entrega
    def actualizar_entregado(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.equipo SET estado=%(status)s , subestado=%(subestado)s where id=%(equipo_id)s """,data)
        self.conn.commit()

    ##al generar la firma de devolucion se adjunta foto del estado del equipo entregado
    def actualizar_firma_devolucion(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.equipo SET estado=%(status)s , subestado=%(subestado)s,
                        ubicacionarchivo=%(ubicacionarchivo)s where id=%(equipo_id)s """,data)
        self.conn.commit()

    def actualizar_entrega_accesorio(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.equipo SET estado=%(status)s , subestado=%(subestado)s where id=%(equipo)s """,data)
        self.conn.commit()

    def actualizar_devolucion(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.equipo SET estado=%(status)s  where id=%(equipo_id)s """,data)
        self.conn.commit()



    def editar_descripcion_equipo(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.equipo SET marca=%(marca)s,modelo=%(modelo)s,serial=%(serial)s,mac_wifi=%(mac_wifi)s,
	        serie=%(serie)s,resolucion=%(resolucion)s,dimensiones=%(dimensiones)s,descripcion=%(descripcion)s,ubicacion=%(ubicacion)s,
	        almacenamiento=%(almacenamiento)s,ram=%(ram)s,estado=%(estado)s,tipo=%(tipo)s, cantidad=%(cantidad)s,
                         subestado=%(subestado)s , ubicacionarchivo=%(ubicacionarchivo)s where id=%(id)s """,data)
        self.conn.commit()

    def editar_persona(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.persona SET nombres=%(nombres)s,apellidos=%(apellidos)s,rut=%(rut)s,
	        nacionalidad=%(nacionalidad)s,fecha_nacimiento=%(fecha_nacimiento)s,estado_civil=%(estado_civil)s,
	        telefono=%(telefono)s,fecha_ingreso=%(fecha_ingreso)s,cargo=%(cargo)s,domicilio=%(domicilio)s,
	        comuna=%(comuna)s,banco=%(banco)s,tipo_cuenta=%(tipo_cuenta)s,numero_cuenta=%(numero_cuenta)s,
	        correo=%(correo)s,afp=%(afp)s,salud=%(salud)s,telefono_adicional=%(telefono_adicional)s,
	        nombre_contacto=%(nombre_contacto)s,seguro_covid=%(seguro_covid)s,horario=%(horario)s,
	        ceco=%(ceco)s,sueldo_base=%(sueldo_base)s,tipo_contrato=%(tipo_contrato)s,direccion_laboral=%(direccion_laboral)s,
	        enfermedad=%(enfermedad)s,polera=%(polera)s,pantalon=%(pantalon)s,poleron=%(poleron)s,
	        zapato=%(zapato)s, req_comp=%(req_comp)s, req_cel=%(req_cel)s  where id=%(id)s """ , data)
        self.conn.commit()

    def datos_acta_entrega(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.asignacion
                    SET  fecha_entrega=%(fecha_entrega)s,folio_entrega=%(folio_entrega)s, estado=%(estado)s, sub_estado=%(sub_estado)s
                    WHERE id=%(id)s; """,data)
        self.conn.commit()

    def datos_acta_devolucion(self, data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.asignacion
                    SET  fecha_devolucion=%(fecha_devolucion)s,folio_devolucion=%(folio_devolucion)s, estado=%(estado)s
                    WHERE id=%(id)s; """,data)
        self.conn.commit()

    #devolucion de un accesorio
        
    def actualizar_devolucion_accesorio(self, data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.asignacion
                    SET  fecha_devolucion=%(fecha_devolucion)s, estado=%(estado)s, sub_estado=%(sub_estado)s
                    WHERE id=%(id_asignacion)s; """,data)   
        self.conn.commit()


    #cambio del estado de firma de entrega
    def firma_acta_entrega(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""UPDATE inventario.asignacion
                        SET firma_entrega=%(firma_entrega)s, 
                        sub_estado=%(sub_estado)s
                    
                        WHERE id=%(id)s """,data)
        self.conn.commit()

    def firma_acta_devolucion(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""UPDATE inventario.asignacion
                        SET firma_devolucion=%(firma_devolucion)s, observacion=%(observacion)s, sub_estado=%(sub_estado)s
                        WHERE id=%(id)s """,data)
        self.conn.commit()

    def actualizar_habilitado(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""UPDATE inventario.persona
                        SET habilitado=%(habilitado)s WHERE id=%(id)s """, data)
        self.conn.commit()
    

    ## actualizar estado de unidades sin etiqueta rsv
    def actualizar_unidad_con_etiqueta(self,data):
        with self.conn.cursor() as cur :
            cur.execute(""" UPDATE rsv.catalogo_productos SET unid_con_etiqueta= %(unid_con_etiqueta)s
                        where  codigo =%(codigo)s """,data)
        self.conn.commit()

    ##lista de unidades sin etiquetas
    def read_unidades_sin_etiqueta_rsv(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                    select p.id, p.created_at , p.codigo, p.producto, p.unid_x_paquete, p.peso, p.ancho, p.alto, p.largo, 
                        c.nombre_color as color, p.precio_unitario,p.ubicacion_p, p.ubicacion_u ,p.codigo_original, 
                        p.id_user , p.ids_user, p.habilitado , p.unid_con_etiqueta  from rsv.catalogo_productos p 
                        inner join rsv.colores c on p.color = c.id 
                        where unid_con_etiqueta  = false""")
            return cur.fetchall()
        
    # /Y


    #NS Picking

    def ns_picking(self, fecha_inicio,fecha_fin):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select * from areati.calcular_nivel_servicio_verificado('{fecha_inicio}','{fecha_fin}')
                union all
                select 'Total',
                        sum(total_registros),
                        sum(productos_verificados),
                        case
                            when  sum(productos_verificados) > 0 then       
                            ROUND((sum(productos_verificados) / sum(total_registros) * 100), 2)
                    else 0
                    end
                from areati.calcular_nivel_servicio_verificado('{fecha_inicio}','{fecha_fin}');
                        """)
            return cur.fetchall()


    ## pendientes y rutas predictivas

    def prueba_ty(self, offset):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select * from rutas.pendientes_seg(null, null,{offset})
                        """)
            return cur.fetchall()
        


    ## pendientes y rutas predictivas
    ### de la funcion select * from rutas.pendientes_seg_v2(null,null,0)
    def fechas_pendientes(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select min(subquery.fec_min) as fec_min,
                max(subquery.fec_max) as fec_max
                from (
                ---------------------------------------------------------------
                -- [H-003] Consolidado Clientes
                ---------------------------------------------------------------
                select 	min(cc.fecha_entrega) as fec_min,
                        max(cc.fecha_entrega) as fec_max
                from rutas.consolidado_clientes cc
                JOIN areati.subestado_entregas s ON cc.estado = s.parent_code AND cc.subestado = s.code
                WHERE s.definitivo = false
                    and cc.guia not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    and cc.guia not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
                ---------------------------------------------------------------
                union all
                select 	min(easy.fecha_entrega) as fec_min,
                        max(easy.fecha_entrega) as fec_max
                from areati.ti_wms_carga_easy easy 
                WHERE (easy.estado = 0 OR (easy.estado = 2 AND easy.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND easy.estado NOT IN (1, 3)
                    and easy.entrega not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    and easy.entrega not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
                union all
                select  min(opl.fec_compromiso) as fec_min,
                        max(opl.fec_compromiso) as fech_max
                from areati.ti_carga_easy_go_opl opl
                WHERE (opl.estado = 0 OR (opl.estado = 2 AND opl.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND opl.estado NOT IN (1, 3)
                    and opl.suborden not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    and opl.suborden not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
                union all
                select  min(rtcl.fecha_pedido) as fec_min,
                        max(rtcl.fecha_pedido) as fec_max
                from areati.ti_retiro_cliente rtcl
                WHERE (rtcl.estado = 0 OR (rtcl.estado = 2 AND rtcl.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND rtcl.estado NOT IN (1, 3)
                    and rtcl.cod_pedido not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    and rtcl.cod_pedido not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
                ) subquery;


                        """)
            return cur.fetchone()
        
    def pendientes_consolidados_clientes_sin_lateral(self, fecha_inicio,fecha_fin, offset ): ### sin busca_ruta_manual
         with self.conn.cursor() as cur:
            cur.execute(f"""
            ------ VERSION MAS "OPTIMA"
            ------ 1  a 3 segundos

            with f_consolidado as (
                            select 	cc.cod_cliente AS "Código de Cliente",
                            cc.nombre AS "Nombre",
                            cc.direccion_corregida as "Calle y Número",
                            cc.direccion_literal as "Dirección Textual",
                            cc.comuna as "Ciudad",
                            cc.region  as "Provincia/Estado",
                            cc.latitud::text as "Latitud",
                            cc.longitud::text as "Longitud",
                            cc.telefono as "Teléfono con código de país",
                            cc.correo_electronico as "Email",
                            cc.guia as "Código de Pedido",
                            cc.fecha_entrega as "Fecha Original Pedido",
                            'E' as "Operación E/R",
                            cc.carton as "Código de Producto",
                            '(' || c.nombre || ') ' || cc.producto as "Descripción del Producto",
                            cc.cantidad as "Cantidad de Producto"
                    from rutas.consolidado_clientes cc
                    left join rutas.clientes c on c.id = cc.cliente
                    --left join public.ti_tamano_sku tts on tts.sku = cast(cc.sku as text)
                -- left join areati.estado_entregas ee on ee.estado=cc.estado
                -- left join beetrack.ruta_transyanez rb on (cc.guia=rb.guia and rb.created_at::date = current_date and lower(rb.cliente) = lower(c.nombre) )
                    --where
                    --cc.fecha_entrega >= '{fecha_inicio}' and cc.fecha_entrega <= '{fecha_fin}'        
                        
                        )
                        
        
            SELECT
                subquery.origen,
                subquery.id_cliente,
                subquery.guia,
                to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
                subquery.fec_entrega,
                funcion_resultado."Provincia/Estado",
                funcion_resultado."Ciudad",
                --funcion_resultado."Descripción del Producto",
                SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
                funcion_resultado."Cantidad de Producto"::int4,
                ee.descripcion as "Estado",
                se."name" as "Subestado",
                subquery.verified,
                subquery.recepcion,
                subquery.observacion,
                subquery.alerta
            FROM (
            ------------------------------------------------------------------------------------------------------
            -- [H-003] select * from rutas.consolidado_clientes cc order by 2 desc;
            ------------------------------------------------------------------------------------------------------
            SELECT DISTINCT ON (cc.guia)
                cc.guia as guia,
                c.nombre as origen,
                cc.cliente as id_cliente,
                cc.created_at as fec_ingreso,
                coalesce(tbm.fecha, cc.fecha_entrega) as fec_entrega,
                cc.comuna,
                cc.estado,
                cc.subestado,
                cc.verificado as verified,
                cc.recepcionado as recepcion,
                tbm.observacion,
                case 
                    when tbm.alerta= true then true
                    else false 
                end as alerta
            FROM rutas.consolidado_clientes cc
            left join rutas.clientes c on c.id = cc.cliente
            LEFT JOIN (
                        SELECT DISTINCT ON (toc.guia) 
                        toc.guia as guia,
                        toc.cliente as cliente,
                        toc.direccion_correcta as direccion, 
                        toc.comuna_correcta as comuna,
                        toc.fec_reprogramada as fecha,
                        toc.observacion,
                        toc.alerta
                        FROM rutas.toc_bitacora_mae toc
                        WHERE toc.alerta = true
                        ORDER BY toc.guia, toc.created_at desc
                    ) AS tbm ON (cc.guia = tbm.guia and c.nombre = tbm.cliente)
            JOIN areati.subestado_entregas s ON cc.estado = s.parent_code AND cc.subestado = s.code
            WHERE s.definitivo = false
            and not exists (
                select rt.guia 
                            from beetrack.ruta_transyanez rt 
                            where rt.created_at::date = current_date 
                            and lower(rt.cliente) = lower(c.nombre)
                            and rt.guia = cc.guia
            )
            and not exists (
                SELECT  drm.cod_pedido
                FROM quadminds.datos_ruta_manual drm
                WHERE drm.estado = true
                AND lower(drm.notas) = lower(c.nombre)
                AND drm.cod_pedido = cc.guia
            )
            and cc.fecha_entrega >= '{fecha_inicio}' and cc.fecha_entrega <= '{fecha_fin}'


            ) subquery
            --JOIN LATERAL areati.busca_ruta_manual_base2(subquery.guia) AS funcion_resultado ON true
            left join f_consolidado funcion_resultado on subquery.guia = funcion_resultado."Código de Pedido"
            left join areati.estado_entregas ee on subquery.estado = ee.estado 
            left join areati.subestado_entregas se on subquery.subestado = se.code 
            where to_char(subquery.fec_entrega,'yyyymmdd')>='{fecha_inicio}'
            and to_char(subquery.fec_entrega,'yyyymmdd')<='{fecha_fin}'

               
                        """)
            return cur.fetchall()
    

    def pendientes_consolidados_clientes(self, fecha_inicio,fecha_fin, offset ): #### con busca_ruta_manual
         with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT
                subquery.origen,
                subquery.id_cliente,
                subquery.guia,
                to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
                funcion_resultado."Fecha de Pedido",
                funcion_resultado."Provincia/Estado",
                funcion_resultado."Ciudad",
                --funcion_resultado."Descripción del Producto",
                SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
                funcion_resultado."Cantidad de Producto"::int4,
                ee.descripcion as "Estado",
                se."name" as "Subestado",
                subquery.verified,
                subquery.recepcion,
                subquery.observacion,
                subquery.alerta
            FROM (
            ------------------------------------------------------------------------------------------------------
            -- [H-003] select * from rutas.consolidado_clientes cc order by 2 desc;
            ------------------------------------------------------------------------------------------------------
            SELECT DISTINCT ON (cc.guia)
                cc.guia as guia,
                c.nombre as origen,
                cc.cliente as id_cliente,
                cc.created_at as fec_ingreso,
                coalesce(tbm.fecha, cc.fecha_entrega) as fec_entrega,
                cc.comuna,
                cc.estado,
                cc.subestado,
                cc.verificado as verified,
                cc.recepcionado as recepcion,
                tbm.observacion,
                case 
                    when tbm.alerta= true then true
                    else false 
                end as alerta
            FROM rutas.consolidado_clientes cc
            left join rutas.clientes c on c.id = cc.cliente
            LEFT JOIN (
                        SELECT DISTINCT ON (toc.guia) 
                        toc.guia as guia,
                        toc.cliente as cliente,
                        toc.direccion_correcta as direccion, 
                        toc.comuna_correcta as comuna,
                        toc.fec_reprogramada as fecha,
                        toc.observacion,
                        toc.alerta
                        FROM rutas.toc_bitacora_mae toc
                        WHERE toc.alerta = true
                        ORDER BY toc.guia, toc.created_at desc
                    ) AS tbm ON (cc.guia = tbm.guia and c.nombre = tbm.cliente)
            JOIN areati.subestado_entregas s ON cc.estado = s.parent_code AND cc.subestado = s.code
            WHERE s.definitivo = false
            and not exists (
                select rt.guia 
                            from beetrack.ruta_transyanez rt 
                            where rt.created_at::date = current_date 
                            and lower(rt.cliente) = lower(c.nombre)
                            and rt.guia = cc.guia
            )
            and not exists (
                SELECT  drm.cod_pedido
                FROM quadminds.datos_ruta_manual drm
                WHERE drm.estado = true
                AND lower(drm.notas) = lower(c.nombre)
                AND drm.cod_pedido = cc.guia
            )
            and cc.fecha_entrega >= '{fecha_inicio}' and cc.fecha_entrega <= '{fecha_fin}'


            ) subquery
            JOIN LATERAL areati.busca_ruta_manual_base2(subquery.guia) AS funcion_resultado ON true
            left join areati.estado_entregas ee on subquery.estado = ee.estado 
            left join areati.subestado_entregas se on subquery.subestado = se.code 
            where to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')>='{fecha_inicio}'
            and to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')<='{fecha_fin}'
               
                        """)
            return cur.fetchall()
    
    
    ## pendientes de sportex y electrolux
    def pendientes_sportex_elux(self, fecha_inicio,fecha_fin):
        with self.conn.cursor() as cur:
            cur.execute(f"""
               --union sportex electrolux 
                SELECT
                    subquery.origen,
                    subquery.guia,
                    to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
                    funcion_resultado."Fecha de Pedido",
                    funcion_resultado."Provincia/Estado",
                    funcion_resultado."Ciudad",
                    --funcion_resultado."Descripción del Producto",
                    SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
                    funcion_resultado."Cantidad de Producto"::int4,
                    ee.descripcion as "Estado",
                    se."name" as "Subestado",
                    subquery.verified,
                    subquery.recepcion,
                    subquery.observacion,
                    subquery.alerta
                FROM (
                    ---SPORTEX 
                    select distinct on (sptx.id_sportex)
                        sptx.id_sportex as guia,
                        'Sportex' as origen,
                        sptx.created_at as fec_ingreso,
                        coalesce(tbm.fecha,
                            case
                                when (select fecha_siguiente from areati.obtener_dias_habiles(to_char(sptx.created_at + interval '1 day','yyyymmdd'))) <> sptx.fecha_entrega
                                then (select fecha_siguiente from areati.obtener_dias_habiles(to_char(sptx.created_at + interval '1 day','yyyymmdd')))
                                else sptx.fecha_entrega
                            end) as fec_entrega,
                        sptx.comuna as comuna,
                        sptx.estado,
                        sptx.subestado, 
                        sptx.verified,
                        sptx.recepcion,
                        '' as observacion,
                        false as alerta  	
                    from areati.ti_wms_carga_sportex sptx
                    LEFT JOIN (
                                SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                                toc.direccion_correcta as direccion, 
                                toc.comuna_correcta as comuna,
                                toc.fec_reprogramada as fecha,
                                toc.observacion,
                                toc.alerta
                                FROM rutas.toc_bitacora_mae toc
                                WHERE toc.alerta = true
                                ORDER BY toc.guia, toc.created_at desc
                            ) AS tbm ON sptx.id_sportex=tbm.guia
                    WHERE (sptx.estado = 0 OR (sptx.estado = 2 AND sptx.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND sptx.estado NOT IN (1, 3)
                    and sptx.id_sportex not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    -- and sptx.id_sportex not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
                    and sptx.id_sportex not in (select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                    and sptx.fecha_entrega >= '{fecha_inicio}' and sptx.fecha_entrega <= '{fecha_fin}'
                    union all
                    ---Electrolux 
                    select distinct on (eltx.numero_guia)
                        eltx.numero_guia as guia,
                        'Electrolux' as origen,
                        eltx.created_at as fec_ingreso,
                        coalesce(tbm.fecha,
                        case
                                when (select fecha_siguiente from areati.obtener_dias_habiles(to_char(eltx.created_at + interval '1 day','yyyymmdd'))) <> eltx.fecha_min_entrega
                                then (select fecha_siguiente from areati.obtener_dias_habiles(to_char(eltx.created_at + interval '1 day','yyyymmdd')))
                                else eltx.fecha_min_entrega
                            end) as fec_entrega,
                        eltx.comuna as comuna,
                        eltx.estado,
                        eltx.subestado, 
                        eltx.verified,
                        eltx.recepcion,
                        tbm.observacion,
                        tbm.alerta    	
                    from areati.ti_wms_carga_electrolux eltx
                    LEFT JOIN (
                                SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                                toc.direccion_correcta as direccion, 
                                toc.comuna_correcta as comuna,
                                toc.fec_reprogramada as fecha,
                                toc.observacion,
                                toc.alerta
                                FROM rutas.toc_bitacora_mae toc
                                WHERE toc.alerta = true
                                ORDER BY toc.guia, toc.created_at desc
                            ) AS tbm ON eltx.numero_guia=tbm.guia
                    WHERE (eltx.estado = 0 OR (eltx.estado = 2 AND eltx.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND eltx.estado NOT IN (1, 3)
                    -- and eltx.numero_guia not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    and eltx.numero_guia not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
                    and eltx.numero_guia not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                    and eltx.fecha_min_entrega >= '{fecha_inicio}' and eltx.fecha_min_entrega <= '{fecha_fin}'
                    
                ) subquery
                JOIN LATERAL areati.busca_ruta_manual_base2(subquery.guia) AS funcion_resultado ON true
                left join areati.estado_entregas ee on subquery.estado = ee.estado 
                left join areati.subestado_entregas se on subquery.subestado = se.code 
                where to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')>= '{fecha_inicio}'
                and to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')<= '{fecha_fin}'
                        """)
            return cur.fetchall()
        
    ## pendientes de Easy OPL
    def pendientes_easy_opl_mio(self, fecha_inicio,fecha_fin, offset ): #### actulalizado con id_cliente
         with self.conn.cursor() as cur:
            cur.execute(f"""

            with f_opl as (
            select  easygo.rut_cliente AS "Código de Cliente",
                    initcap(easygo.nombre_cliente) AS "Nombre",
                    coalesce(tbm.direccion,
                    CASE 
                        WHEN substring(easygo.direc_despacho from '^\d') ~ '\d' then substring(initcap(easygo.direc_despacho) from '\d+[\w\s]+\d+')
                        WHEN lower(easygo.direc_despacho) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                        regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easygo.direc_despacho,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                        else coalesce(substring(initcap(easygo.direc_despacho) from '^[^0-9]*[0-9]+'),initcap(easygo.direc_despacho))
                    end) as "Calle y Número",
                    
                    coalesce(tbm.direccion,easygo.direc_despacho) as "Dirección Textual",
                    
                    coalesce(tbm.comuna,
                    case
                    when unaccent(lower(easygo.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                    (select oc.comuna_name from public.op_comunas oc 
                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easygo.comuna_despacho))
                    )
                        )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                    where unaccent(lower(easygo.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                    )
                    end) as "Ciudad",
                    
                    case
                        when unaccent(lower(easygo.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select opr.region_name  from public.op_regiones opr 
                        where opr.id_region = (select oc.id_region from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                            where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easygo.comuna_despacho))
                            )	
                        ))
                        else(select opr.region_name  from public.op_regiones opr 
                        where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                        where unaccent(lower(easygo.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                        ))
                    end as "Provincia/Estado",
                    CAST (easygo.suborden AS varchar) AS "Código de Pedido",
                    coalesce(tbm.fecha,easygo.fec_compromiso) AS "Fecha de Pedido",
                    '(Easy OPL) ' || coalesce(REPLACE(easygo.descripcion, ',', ''),'') AS "Descripción del Producto",
                    cast(easygo.unidades as numeric) AS "Cantidad de Producto",
                    'Easy OPL' AS "Notas"

            from areati.ti_carga_easy_go_opl easygo
            left join ti_comuna_region tcr on
                translate(lower(easygo.comuna_despacho),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
            --left join public.ti_tamano_sku tts on tts.sku = cast(easygo.codigo_sku as text)
            -- join areati.estado_entregas ee on ee.estado=easygo.estado

            LEFT JOIN (
                SELECT DISTINCT ON (guia) guia as guia, 
                direccion_correcta as direccion, 
                comuna_correcta as comuna,
                fec_reprogramada as fecha,
                observacion,
                alerta
                FROM rutas.toc_bitacora_mae
                WHERE alerta = true
                ORDER BY guia, created_at desc
            ) AS tbm ON easygo.suborden=tbm.guia
)

                
                SELECT
                    subquery.origen,
                    subquery.id_cliente,
                    subquery.guia,
                    to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
                    funcion_resultado."Fecha de Pedido",
                    funcion_resultado."Provincia/Estado",
                    funcion_resultado."Ciudad",
                    --funcion_resultado."Descripción del Producto",
                    SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
                    funcion_resultado."Cantidad de Producto"::int4,
                    ee.descripcion as "Estado",
                    se."name" as "Subestado",
                    subquery.verified,
                    subquery.recepcion,
                    subquery.observacion,
                    subquery.alerta
                FROM (
                ---EASY OPL 
                select distinct on (opl.suborden)
                        opl.suborden as guia,
                        'Tienda Easy' as origen,
                        3 as id_cliente,
                        opl.created_at as fec_ingreso,
                        coalesce(tbm.fecha,opl.fec_compromiso) as fec_entrega,
                        opl.comuna_despacho as comuna,
                        opl.estado,
                        opl.subestado, 
                        opl.verified,
                        opl.recepcion,
                        tbm.observacion,
                        case 
				        	when tbm.alerta = true then true
				        	else false 
				        end as alerta
                    from areati.ti_carga_easy_go_opl opl
                    LEFT JOIN (
                                SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                                toc.direccion_correcta as direccion, 
                                toc.comuna_correcta as comuna,
                                toc.fec_reprogramada as fecha,
                                toc.observacion,
                                toc.alerta
                                FROM rutas.toc_bitacora_mae toc
                                WHERE toc.alerta = true
                                ORDER BY toc.guia, toc.created_at desc
                            ) AS tbm ON opl.suborden=tbm.guia
                    WHERE (opl.estado = 0 OR (opl.estado = 2 AND opl.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND opl.estado NOT IN (1, 3)
                    -- and opl.suborden not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    and opl.suborden not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                    and opl.suborden not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
                    and opl.fec_compromiso >= '{fecha_inicio}' and opl.fec_compromiso <= '{fecha_fin}'
                    --limit 100 offset 0
                ) subquery
                --JOIN LATERAL areati.busca_ruta_manual_base2(subquery.guia) AS funcion_resultado ON true
                left join f_opl funcion_resultado  on subquery.guia = funcion_resultado."Código de Pedido"
                left join areati.estado_entregas ee on subquery.estado = ee.estado 
                left join areati.subestado_entregas se on subquery.subestado = se.code 
                where to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')>= '{fecha_inicio}'
                and to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')<= '{fecha_fin}'
                        """)
            return cur.fetchall()
        
    def pendientes_easy_opl(self, fecha_inicio,fecha_fin, offset ):
         with self.conn.cursor() as cur:
            cur.execute(f"""
                --EASY OPL (QUIZA CON LIMIT 100 OFFSET '')
                SELECT
                    subquery.origen,
                    subquery.guia,
                    to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
                    funcion_resultado."Fecha de Pedido",
                    funcion_resultado."Provincia/Estado",
                    funcion_resultado."Ciudad",
                    --funcion_resultado."Descripción del Producto",
                    SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
                    funcion_resultado."Cantidad de Producto"::int4,
                    ee.descripcion as "Estado",
                    se."name" as "Subestado",
                    subquery.verified,
                    subquery.recepcion
                FROM (
                ---EASY OPL 
                select distinct on (opl.suborden)
                        opl.suborden as guia,
                        'Tienda Easy' as origen,
                        opl.created_at as fec_ingreso,
                        coalesce(tbm.fecha,opl.fec_compromiso) as fec_entrega,
                        opl.comuna_despacho as comuna,
                        opl.estado,
                        opl.subestado, 
                        opl.verified,
                        opl.recepcion    	
                    from areati.ti_carga_easy_go_opl opl
                    LEFT JOIN (
                                SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                                toc.direccion_correcta as direccion, 
                                toc.comuna_correcta as comuna,
                                toc.fec_reprogramada as fecha,
                                toc.observacion,
                                toc.alerta
                                FROM rutas.toc_bitacora_mae toc
                                WHERE toc.alerta = true
                                ORDER BY toc.guia, toc.created_at desc
                            ) AS tbm ON opl.suborden=tbm.guia
                    WHERE (opl.estado = 0 OR (opl.estado = 2 AND opl.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND opl.estado NOT IN (1, 3)
                    -- and opl.suborden not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    and opl.suborden not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                    and opl.suborden not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
                    and opl.fec_compromiso >= '{fecha_inicio}' and opl.fec_compromiso <= '{fecha_fin}'
                    --limit 100 offset 0
                ) subquery
                JOIN LATERAL areati.busca_ruta_manual_base2(subquery.guia) AS funcion_resultado ON true
                left join areati.estado_entregas ee on subquery.estado = ee.estado 
                left join areati.subestado_entregas se on subquery.subestado = se.code 
                where to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')>= '{fecha_inicio}'
                and to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')<= '{fecha_fin}'
               
                        """)
            return cur.fetchall()
         

        ## pendientes de retiro tienda
       
    
    def pendientes_retiro_tienda(self, fecha_inicio,fecha_fin, offset ):  #### actulalizado con id_cliente
         with self.conn.cursor() as cur:
            cur.execute(f"""
               SELECT
                    subquery.origen,
                    subquery.id_cliente,
                    subquery.guia,
                    to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
                    funcion_resultado."Fecha de Pedido",
                    funcion_resultado."Provincia/Estado",
                    funcion_resultado."Ciudad",
                    --funcion_resultado."Descripción del Producto",
                    SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
                    funcion_resultado."Cantidad de Producto"::int4,
                    ee.descripcion as "Estado",
                    se."name" as "Subestado",
                    subquery.verified,
                    subquery.recepcion,
                    subquery.observacion,
                    subquery.alerta
                FROM (
                                select distinct on (rtcl.cod_pedido)
                        rtcl.cod_pedido as guia,
                        'Envio/Retiro' as origen,
                        0 as id_cliente,
                        rtcl.created_at as fec_ingreso,
                        coalesce(tbm.fecha,rtcl.fecha_pedido) as fec_entrega,
                        rtcl.comuna as comuna,
                        rtcl.estado,
                        rtcl.subestado, 
                        rtcl.verified,
                        rtcl.verified as recepcion,
                        tbm.observacion,
                        case 
                            when tbm.alerta= true then true
                            else false 
                        end as alerta    	
                    from areati.ti_retiro_cliente rtcl
                    LEFT JOIN (
                                SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                                toc.direccion_correcta as direccion, 
                                toc.comuna_correcta as comuna,
                                toc.fec_reprogramada as fecha,
                                toc.observacion,
                                toc.alerta
                                FROM rutas.toc_bitacora_mae toc
                                WHERE toc.alerta = true
                                ORDER BY toc.guia, toc.created_at desc
                            ) AS tbm ON rtcl.cod_pedido=tbm.guia
                    WHERE (rtcl.estado = 0 OR (rtcl.estado = 2 AND rtcl.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND rtcl.estado NOT IN (1, 3)
                    -- and rtcl.cod_pedido not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    and rtcl.cod_pedido not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                    and rtcl.cod_pedido not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
                    and rtcl.fecha_pedido >= '{fecha_inicio}' and rtcl.fecha_pedido <= '{fecha_fin}'
                        --limit 100 offset 0
                    ) subquery
                    JOIN LATERAL areati.busca_ruta_manual_base2(subquery.guia) AS funcion_resultado ON true
                    left join areati.estado_entregas ee on subquery.estado = ee.estado 
                    left join areati.subestado_entregas se on subquery.subestado = se.code 
                    where to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')>= '{fecha_inicio}'
                    and to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')<= '{fecha_fin}'
                        """)
            return cur.fetchall()
         
      ## pendientes de Retiro tienda
    
    def pendientes_retiro_tienda_mio(self, fecha_inicio,fecha_fin, offset ):
         with self.conn.cursor() as cur:
            cur.execute(f"""
               with f_aux as(
                select  --retc.envio_asociado AS "Código de Cliente",
                        null AS "Código de Cliente",
                        initcap(retc.nombre_cliente) AS "Nombre",
                        coalesce(tbm.direccion,
                        CASE 
                            WHEN substring(retc.direccion from '^\d') ~ '\d' then substring(initcap(retc.direccion) from '\d+[\w\s]+\d+')
                            WHEN lower(retc.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                            regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(retc.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                            else coalesce(substring(initcap(retc.direccion) from '^[^0-9]*[0-9]+'),initcap(retc.direccion))
                        end) as "Calle y Número",
                        coalesce(tbm.direccion,retc.direccion) as "Dirección Textual",
                        coalesce(tbm.comuna,
                        case
                        when unaccent(lower(retc.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                
                        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(retc.comuna))
                        )
                            )
                            else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                        where unaccent(lower(retc.comuna)) = unaccent(lower(oc2.comuna_name))
                        )
                        end) as "Ciudad",
                        case
                            when unaccent(lower(retc.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select opr.region_name  from public.op_regiones opr 
                            where opr.id_region = (select oc.id_region from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(retc.comuna))
                                )    
                            ))
                            else(select opr.region_name  from public.op_regiones opr 
                            where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                            where unaccent(lower(retc.comuna)) = unaccent(lower(oc2.comuna_name))
                            ))
                        end as "Provincia/Estado",
                        '' AS "Latitud",
                        '' AS "Longitud",
                        coalesce(retc.telefono ,'0') AS "Teléfono con código de país",
                        lower(retc.email) AS "Email",
                        CAST (retc.cod_pedido AS varchar) AS "Código de Pedido",
                        coalesce(tbm.fecha,retc.fecha_pedido) AS "Fecha de Pedido",
                        INITCAP(LEFT(retc.tipo, 1)) AS "Operación E/R",
                        retc.cod_pedido AS "Código de Producto",
                        '(' || initcap(retc.cliente) || ') ' || coalesce(REPLACE(retc.descripcion, ',', ''),'') AS "Descripción del Producto",
                        cast(retc.bultos as numeric) AS "Cantidad de Producto",
                        1 AS "Peso",
                        1 AS "Volumen",
                        1 AS "Dinero",
                        '8' AS "Duración min",
                        '09:00 - 21:00' AS "Ventana horaria 1",
                        '' AS "Ventana horaria 2",
                        coalesce((select initcap(retc.cliente) || ' ' || se.name from areati.subestado_entregas se 
                                where retc.subestado=se.code 
                                and retc.estado=se.parent_code), initcap(retc.cliente)) AS "Notas",
                        case
                        when tcr.region = 'Region Metropolitana' then 'RM - ' || coalesce (tts.tamano,'?')
                        when tcr.region = 'Valparaíso' then 'V - ' ||  initcap(retc.comuna)
                        END "Agrupador",
                        '' AS "Email de Remitentes",
                        '' AS "Eliminar Pedido Si - No - Vacío",
                        '' AS "Vehículo",
                        '' AS "Habilidades",
                    cast(retc.sku as text) as "Cod. SKU",                    -- no va a Quadminds 
                    CASE
                            WHEN retc.verified = TRUE THEN TRUE
                            ELSE FALSE
                    END as "Pistoleado",
                    --retc.verified as "Pistoleado",                         -- no va a Quadminds
                    coalesce (tts.tamano,'?') as "Talla",                    -- no va a Quadminds
                    -- initcap(ee.descripcion) as "Estado Entrega",       	-- no va a Quadminds
                    case
                            when retc.estado=2 and retc.subestado in (7,10,12,19,43,44,50,51,70,80)
                            then 'NO SACAR A RUTA'
                            when retc.estado=3 
                            then 'NO SACAR A RUTA'
                            else initcap(ee.descripcion)
                    end as "Estado Entrega",
                    COALESCE(rb.identificador_ruta IS NOT NULL, false) as "En Ruta",    -- no va a Quadminds
                    tbm.alerta as "TOC",                                             -- Alertado por TOC
                    tbm.observacion as "Observacion TOC",                            -- Alertado por TOC
                    case
                            when (retc.estado=2 and retc.subestado in (7,10,12,19,43,44,50,51,70,80)) or (retc.estado=3)
                            then true
                            else false 
                    end AS "Sistema",	                                            -- Alertado por el Sistema
                    case
                            when retc.estado=2 and retc.subestado in (7,10,12,19,43,44,50,51,70,80)
                            then (select name from areati.subestado_entregas where parent_code=2 and code=retc.subestado) || CHR(10)
                            when retc.estado=3 
                            then 'Archivado' || CHR(10)
                            else ''
                    end AS "Obs. Sistema"                                             -- Alertado por el Sistema
                from areati.ti_retiro_cliente retc
                left join ti_comuna_region tcr on
                    translate(lower(retc.comuna),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
                left join public.ti_tamano_sku tts on tts.sku = cast(retc.sku as text)
                left join areati.estado_entregas ee on ee.estado=retc.estado
                --left join quadminds.ti_respuesta_beetrack rb on retc.cod_pedido=rb.guia
                left join beetrack.ruta_transyanez rb on (retc.cod_pedido=rb.guia and rb.created_at::date = current_date)
                LEFT JOIN (
                    SELECT DISTINCT ON (guia) guia as guia, 
                    direccion_correcta as direccion, 
                    comuna_correcta as comuna,
                    fec_reprogramada as fecha,
                    observacion,
                    alerta
                    FROM rutas.toc_bitacora_mae
                    WHERE alerta = true
                    ORDER BY guia, created_at desc
                ) AS tbm on retc.cod_pedido=tbm.guia
                )

                SELECT
                    subquery.origen,
                    subquery.guia,
                    to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
                    funcion_resultado."Fecha de Pedido",
                    funcion_resultado."Provincia/Estado",
                    funcion_resultado."Ciudad",
                    --funcion_resultado."Descripción del Producto",
                    SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
                    funcion_resultado."Cantidad de Producto"::int4,
                    ee.descripcion as "Estado",
                    se."name" as "Subestado",
                    subquery.verified,
                    subquery.recepcion,
                    subquery.observacion,
                    subquery.alerta
                FROM (
                select distinct on (rtcl.cod_pedido)
                        rtcl.cod_pedido as guia,
                        'Envio/Retiro' as origen,
                        rtcl.created_at as fec_ingreso,
                        coalesce(tbm.fecha,rtcl.fecha_pedido) as fec_entrega,
                        rtcl.comuna as comuna,
                        rtcl.estado,
                        rtcl.subestado, 
                        rtcl.verified,
                        rtcl.verified as recepcion,
                        tbm.observacion,
                        case 
				        	when tbm.alerta = true then true
				        	else false 
				        end as alerta
                    from areati.ti_retiro_cliente rtcl
                    LEFT JOIN (
                                SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                                toc.direccion_correcta as direccion, 
                                toc.comuna_correcta as comuna,
                                toc.fec_reprogramada as fecha,
                                toc.observacion,
                                toc.alerta
                                FROM rutas.toc_bitacora_mae toc
                                WHERE toc.alerta = true
                                ORDER BY toc.guia, toc.created_at desc
                            ) AS tbm ON rtcl.cod_pedido=tbm.guia
                    WHERE (rtcl.estado = 0 OR (rtcl.estado = 2 AND rtcl.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND rtcl.estado NOT IN (1, 3)
                    -- and rtcl.cod_pedido not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    and rtcl.cod_pedido not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                    and rtcl.cod_pedido not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
                    and rtcl.fecha_pedido >= '{fecha_inicio}' and rtcl.fecha_pedido <= '{fecha_fin}'
                    --limit 100 offset 0
                ) subquery
                --JOIN LATERAL areati.busca_ruta_manual_base2(subquery.guia) AS funcion_resultado ON true
                left join f_aux funcion_resultado on subquery.guia = funcion_resultado."Código de Pedido"
                left join areati.estado_entregas ee on subquery.estado = ee.estado 
                left join areati.subestado_entregas se on subquery.subestado = se.code 
                where to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')>= '{fecha_inicio}'
                and to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')<= '{fecha_fin}'
                        """)
            return cur.fetchall()
         


    

    ## pendientes de Easy CD
    def pendientes_easy_cd(self, fecha_inicio,fecha_fin, offset ):
         with self.conn.cursor() as cur:
            cur.execute(f"""
               -- EASY CD (LIMIT 100 OFFSET)
                SELECT
                    subquery.origen,
                    subquery.guia,
                    to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
                    funcion_resultado."Fecha de Pedido",
                    funcion_resultado."Provincia/Estado",
                    funcion_resultado."Ciudad",
                    --funcion_resultado."Descripción del Producto",
                    SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
                    funcion_resultado."Cantidad de Producto"::int4,
                    ee.descripcion as "Estado",
                    se."name" as "Subestado",
                    subquery.verified,
                    subquery.recepcion
                FROM (

                SELECT DISTINCT ON (easy.entrega)
                        easy.entrega as guia,
                        'Easy' as origen,
                        easy.created_at as fec_ingreso,
                        --easy.fecha_entrega as fec_entrega,
                        coalesce(tbm.fecha,
                                CASE
                                    WHEN twcep.fecha_entrega <> easy.fecha_entrega THEN twcep.fecha_entrega
                                    ELSE easy.fecha_entrega
                                END
                                ) as fec_entrega,
                        easy.comuna,
                        easy.estado,
                        easy.subestado,
                        easy.verified,
                        easy.recepcion 
                    FROM areati.ti_wms_carga_easy easy
                    left join public.ti_wms_carga_easy_paso twcep on twcep.entrega = easy.entrega
                    LEFT JOIN (
                                SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                                toc.direccion_correcta as direccion, 
                                toc.comuna_correcta as comuna,
                                toc.fec_reprogramada as fecha,
                                toc.observacion,
                                toc.alerta
                                FROM rutas.toc_bitacora_mae toc
                                WHERE toc.alerta = true
                                ORDER BY toc.guia, toc.created_at desc
                            ) AS tbm ON easy.entrega=tbm.guia
                    WHERE (easy.estado = 0 OR (easy.estado = 2 AND easy.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND easy.estado NOT IN (1, 3)
                    --and easy.entrega not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    and easy.entrega not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                    and easy.entrega not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true) 
                    and easy.fecha_entrega >= '{fecha_inicio}' and easy.fecha_entrega <= '{fecha_fin}'
                    limit 100 offset {offset}
                ) subquery
                JOIN LATERAL areati.busca_ruta_manual_base2(subquery.guia) AS funcion_resultado ON true
                left join areati.estado_entregas ee on subquery.estado = ee.estado 
                left join areati.subestado_entregas se on subquery.subestado = se.code 
                where to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')>= '{fecha_inicio}'
                and to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')<= '{fecha_fin}'
                        """)
            
            return cur.fetchall()

    def pendientes_easy_cd_mio(self, fecha_inicio,fecha_fin, offset ):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            with f_aux as (
            SELECT DISTINCT ON (easy.carton)
                    easy.entrega as guia,
                    'Easy' as origen,
                    easy.created_at as fec_ingreso,
                    --easy.fecha_entrega as fec_entrega,
                    coalesce(tbm.comuna,     
                    case
                                when unaccent(lower(easy.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select oc.comuna_name from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna))
                                                                )
                                )
                                else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                        where unaccent(lower(easy.comuna)) = unaccent(lower(oc2.comuna_name))
                                        )
                        end) as "Ciudad",
                    coalesce(tbm.fecha,
                            CASE
                                WHEN twcep.fecha_entrega <> easy.fecha_entrega THEN twcep.fecha_entrega
                                ELSE easy.fecha_entrega
                            END
                            ) as "Fecha de Pedido",
                            
                    case
                        when unaccent(lower(easy.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select opr.region_name  from public.op_regiones opr 
                        where opr.id_region = (select oc.id_region from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                            where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna))
                            )	
                        ))
                        else(select opr.region_name  from public.op_regiones opr 
                        where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                        where unaccent(lower(easy.comuna)) = unaccent(lower(oc2.comuna_name))
                        ))
                    end as "Provincia/Estado",
                    '(EASY) ' || REPLACE(easy.descripcion, ',', '') AS "Descripción del Producto",
                    CASE 
                            WHEN easy.cant ~ '^\d+$' THEN (select count(*) 
                                                            from areati.ti_wms_carga_easy easy_a 
                                                            where easy_a.entrega = easy.entrega and easy_a.carton=easy.carton) 
                            -- Si el campo es solo un número
                            ELSE regexp_replace(easy.cant, '[^\d]', '', 'g')::numeric 
                            -- Si el campo contiene una frase con cantidad
                        END as "Cantidad de Producto",
                    easy.comuna,
                    easy.estado,
                    easy.subestado,
                    easy.verified,
                    easy.recepcion 
                FROM areati.ti_wms_carga_easy easy
                left join public.ti_wms_carga_easy_paso twcep on twcep.entrega = easy.entrega
                LEFT JOIN (
                            SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                            toc.direccion_correcta as direccion, 
                            toc.comuna_correcta as comuna,
                            toc.fec_reprogramada as fecha,
                            toc.observacion,
                            toc.alerta
                            FROM rutas.toc_bitacora_mae toc
                            WHERE toc.alerta = true
                            ORDER BY toc.guia, toc.created_at desc
                        ) AS tbm ON easy.entrega=tbm.guia
                WHERE (easy.estado = 0 OR (easy.estado = 2 AND easy.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                AND easy.estado NOT IN (1, 3)
                --and easy.entrega not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                and easy.entrega not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                and easy.entrega not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true) 
                and easy.fecha_entrega >= '{fecha_inicio}' and easy.fecha_entrega <= '{fecha_fin}'
            --limit 100 offset 0
            )


            SELECT
                subquery.origen,
                subquery.id_cliente,
                subquery.guia,
                to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
                funcion_resultado."Fecha de Pedido",
                funcion_resultado."Provincia/Estado",
                funcion_resultado."Ciudad",
                --funcion_resultado."Descripción del Producto",
                SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
                funcion_resultado."Cantidad de Producto"::int4,
                ee.descripcion as "Estado",
                se."name" as "Subestado",
                subquery.verified,
                subquery.recepcion,
                subquery.observacion,
                subquery.alerta
            FROM (
                SELECT DISTINCT ON (easy.entrega)
                    easy.entrega as guia,
                    'Easy' as origen,
                    2 as id_cliente,
                    easy.created_at as fec_ingreso,
                    --easy.fecha_entrega as fec_entrega,
                    coalesce(tbm.fecha,
                            CASE
                                WHEN twcep.fecha_entrega <> easy.fecha_entrega THEN twcep.fecha_entrega
                                ELSE easy.fecha_entrega
                            END
                            ) as fec_entrega,
                    easy.comuna,
                    easy.estado,
                    easy.subestado,
                    easy.verified,
                    easy.recepcion,
                    tbm.observacion,
                    case 
                        when tbm.alerta = true then true
                        else false 
                    end as alerta
                FROM areati.ti_wms_carga_easy easy
                left join public.ti_wms_carga_easy_paso twcep on twcep.entrega = easy.entrega
                LEFT JOIN (
                            SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                            toc.direccion_correcta as direccion, 
                            toc.comuna_correcta as comuna,
                            toc.fec_reprogramada as fecha,
                            toc.observacion,
                            toc.alerta
                            FROM rutas.toc_bitacora_mae toc
                            WHERE toc.alerta = true
                            ORDER BY toc.guia, toc.created_at desc
                        ) AS tbm ON easy.entrega=tbm.guia
                WHERE (easy.estado = 0 OR (easy.estado = 2 AND easy.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                AND easy.estado NOT IN (1, 3)
                --and easy.entrega not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                and easy.entrega not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                and easy.entrega not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true) 
                and easy.fecha_entrega >= '{fecha_inicio}' and easy.fecha_entrega <= '{fecha_fin}'
                --limit 100 offset 100
                ) subquery
                left join f_aux funcion_resultado on subquery.guia = funcion_resultado.guia
            -- JOIN LATERAL areati.busca_ruta_manual_base2(subquery.guia) AS funcion_resultado ON true
                left join areati.estado_entregas ee on subquery.estado = ee.estado 
                left join areati.subestado_entregas se on subquery.subestado = se.code 
                where to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')>='{fecha_inicio}'
                and to_char(funcion_resultado."Fecha de Pedido",'yyyy-mm-dd')<='{fecha_fin}'

            """)
            return cur.fetchall()
        

        
    ### PENDIENTES EN RUTA (PENDEJOS)
    ##pendientes en ruta full puro

    def pendientes_en_ruta_full(self, fecha_inicio,fecha_fin, offset ):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT
            subquery.origen,
            subquery.guia,
            to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
            funcion_resultado."Fecha de Pedido",
            funcion_resultado."Provincia/Estado",
            funcion_resultado."Ciudad",
            --funcion_resultado."Descripción del Producto",
            SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
            funcion_resultado."Cantidad de Producto"::int4,
            ee.descripcion as "Estado",
            se."name" as "Subestado",
            subquery.verified,
            subquery.recepcion,
            subquery.nombre_ruta
        FROM (
            SELECT DISTINCT ON (easy.entrega)
                easy.entrega as guia,
                'Easy' as origen,
                easy.created_at as fec_ingreso,
                --easy.fecha_entrega as fec_entrega,
                coalesce(tbm.fecha,
                        CASE
                            WHEN twcep.fecha_entrega <> easy.fecha_entrega THEN twcep.fecha_entrega
                            ELSE easy.fecha_entrega
                        END
                        ) as fec_entrega,
                easy.comuna,
                easy.estado,
                easy.subestado,
                easy.verified,
                easy.recepcion,
                drm.nombre_ruta
            FROM areati.ti_wms_carga_easy easy
            left join public.ti_wms_carga_easy_paso twcep on twcep.entrega = easy.entrega
            LEFT JOIN (
                        SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                        toc.direccion_correcta as direccion, 
                        toc.comuna_correcta as comuna,
                        toc.fec_reprogramada as fecha,
                        toc.observacion,
                        toc.alerta
                        FROM rutas.toc_bitacora_mae toc
                        WHERE toc.alerta = true
                        ORDER BY toc.guia, toc.created_at desc
                    ) AS tbm ON easy.entrega=tbm.guia
            left join quadminds.datos_ruta_manual drm on (drm.cod_pedido = easy.entrega and drm.estado = true )
            WHERE (easy.estado = 0 OR (easy.estado = 2 AND easy.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
            AND easy.estado NOT IN (1, 3)
            and easy.entrega not in (select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
            and easy.fecha_entrega >= '{fecha_inicio}' and easy.fecha_entrega <= '{fecha_fin}'
            ------------------------------------------------------------
            -- (2) Electrolux
            ------------------------------------------------------------
            union all
            select distinct on (eltx.numero_guia)
                eltx.numero_guia as guia,
                'Electrolux' as origen,
                eltx.created_at as fec_ingreso,
                coalesce(tbm.fecha,
                case
                        when (select fecha_siguiente from areati.obtener_dias_habiles(to_char(eltx.created_at + interval '1 day','yyyymmdd'))) <> eltx.fecha_min_entrega
                        then (select fecha_siguiente from areati.obtener_dias_habiles(to_char(eltx.created_at + interval '1 day','yyyymmdd')))
                        else eltx.fecha_min_entrega
                    end) as fec_entrega,
                eltx.comuna as comuna,
                eltx.estado,
                eltx.subestado, 
                eltx.verified,
                eltx.recepcion,
                drm.nombre_ruta
            from areati.ti_wms_carga_electrolux eltx
            LEFT JOIN (
                        SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                        toc.direccion_correcta as direccion, 
                        toc.comuna_correcta as comuna,
                        toc.fec_reprogramada as fecha,
                        toc.observacion,
                        toc.alerta
                        FROM rutas.toc_bitacora_mae toc
                        WHERE toc.alerta = true
                        ORDER BY toc.guia, toc.created_at desc
                    ) AS tbm ON eltx.numero_guia=tbm.guia
            left join quadminds.datos_ruta_manual drm on (drm.cod_pedido = eltx.numero_guia and drm.estado = true )
            WHERE (eltx.estado = 0 OR (eltx.estado = 2 AND eltx.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
            AND eltx.estado NOT IN (1, 3)
            and eltx.numero_guia not in (select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
            and eltx.fecha_min_entrega >= '{fecha_inicio}' and eltx.fecha_min_entrega <= '{fecha_fin}'
            ------------------------------------------------------------
            -- (3) Tienda Easy
            ------------------------------------------------------------
            union all
            select distinct on (opl.suborden)
                opl.suborden as guia,
                'Tienda Easy' as origen,
                opl.created_at as fec_ingreso,
                coalesce(tbm.fecha,opl.fec_compromiso) as fec_entrega,
                opl.comuna_despacho as comuna,
                opl.estado,
                opl.subestado, 
                opl.verified,
                opl.recepcion,
                drm.nombre_ruta
            from areati.ti_carga_easy_go_opl opl
            LEFT JOIN (
                        SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                        toc.direccion_correcta as direccion, 
                        toc.comuna_correcta as comuna,
                        toc.fec_reprogramada as fecha,
                        toc.observacion,
                        toc.alerta
                        FROM rutas.toc_bitacora_mae toc
                        WHERE toc.alerta = true
                        ORDER BY toc.guia, toc.created_at desc
                    ) AS tbm ON opl.suborden=tbm.guia
            left join quadminds.datos_ruta_manual drm on (drm.cod_pedido = opl.suborden and drm.estado = true )
            WHERE (opl.estado = 0 OR (opl.estado = 2 AND opl.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
            AND opl.estado NOT IN (1, 3)
            and opl.suborden not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
            and opl.fec_compromiso >= '{fecha_inicio}' and opl.fec_compromiso <= '{fecha_fin}'
            ------------------------------------------------------------
            -- (4) Ingreso Manual
            ------------------------------------------------------------
            union all
            select distinct on (rtcl.cod_pedido)
                rtcl.cod_pedido as guia,
                'Envio/Retiro' as origen,
                rtcl.created_at as fec_ingreso,
                coalesce(tbm.fecha,rtcl.fecha_pedido) as fec_entrega,
                rtcl.comuna as comuna,
                rtcl.estado,
                rtcl.subestado, 
                rtcl.verified,
                rtcl.verified,
                drm.nombre_ruta
            from areati.ti_retiro_cliente rtcl
            LEFT JOIN (
                        SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                        toc.direccion_correcta as direccion, 
                        toc.comuna_correcta as comuna,
                        toc.fec_reprogramada as fecha,
                        toc.observacion,
                        toc.alerta
                        FROM rutas.toc_bitacora_mae toc
                        WHERE toc.alerta = true
                        ORDER BY toc.guia, toc.created_at desc
                    ) AS tbm ON rtcl.cod_pedido=tbm.guia
            left join quadminds.datos_ruta_manual drm on (drm.cod_pedido = rtcl.cod_pedido and drm.estado = true )
            WHERE (rtcl.estado = 0 OR (rtcl.estado = 2 AND rtcl.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
            AND rtcl.estado NOT IN (1, 3)
            and rtcl.cod_pedido not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
            and rtcl.fecha_pedido >= '{fecha_inicio}' and rtcl.fecha_pedido <= '{fecha_fin}'
            ------------------------------------------------------------
            --limit 100 offset cuenta
        ) subquery
        JOIN LATERAL areati.busca_ruta_manual(subquery.guia) AS funcion_resultado ON true
        left join areati.estado_entregas ee on subquery.estado = ee.estado 
        left join areati.subestado_entregas se on subquery.subestado = se.code 
        where to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')>='{fecha_inicio}'
        and to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')<='{fecha_fin}'
        --order by subquery.fec_ingreso asc

            """)
            return cur.fetchall()

    def pendientes_en_ruta_easy_cd(self, fecha_inicio,fecha_fin, offset ):
        with self.conn.cursor() as cur:
            cur.execute(f"""
       with f_aux as (
                        SELECT DISTINCT ON (easy.carton)
                                easy.entrega as guia,
                                'Easy' as origen,
                                easy.created_at as fec_ingreso,
                                --easy.fecha_entrega as fec_entrega,
                                coalesce(tbm.comuna,     
                                case
                                            when unaccent(lower(easy.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                            (select oc.comuna_name from public.op_comunas oc 
                                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna))
                                                                            )
                                            )
                                            else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                                    where unaccent(lower(easy.comuna)) = unaccent(lower(oc2.comuna_name))
                                                    )
                                    end) as "Ciudad",
                                    coalesce(
								        tbm.direccion,
								        CASE 
									        WHEN substring(easy.direccion from '^\d') ~ '\d' then substring(initcap(easy.direccion) from '\d+[\w\s]+\d+')
									        WHEN lower(easy.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
									        regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easy.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
									        else coalesce(substring(initcap(easy.direccion) from '^[^0-9]*[0-9]+'),initcap(easy.direccion))
								        end) as "Calle y Número",
                                coalesce(tbm.direccion,easy.direccion) as "Dirección Textual",
                                coalesce(tbm.fecha,
                                        CASE
                                            WHEN twcep.fecha_entrega <> easy.fecha_entrega THEN twcep.fecha_entrega
                                            ELSE easy.fecha_entrega
                                        END
                                        ) as "Fecha de Pedido",
                        
                                        
                                case
                                    when unaccent(lower(easy.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                    (select opr.region_name  from public.op_regiones opr 
                                    where opr.id_region = (select oc.id_region from public.op_comunas oc 
                                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easy.comuna))
                                        )	
                                    ))
                                    else(select opr.region_name  from public.op_regiones opr 
                                    where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                                    where unaccent(lower(easy.comuna)) = unaccent(lower(oc2.comuna_name))
                                    ))
                                end as "Provincia/Estado",
                                '(EASY) ' || REPLACE(easy.descripcion, ',', '') AS "Descripción del Producto",
                                CASE 
                                        WHEN easy.cant ~ '^\d+$' THEN (select count(*) 
                                                                        from areati.ti_wms_carga_easy easy_a 
                                                                        where easy_a.entrega = easy.entrega and easy_a.carton=easy.carton) 
                                        -- Si el campo es solo un número
                                        ELSE regexp_replace(easy.cant, '[^\d]', '', 'g')::numeric 
                                        -- Si el campo contiene una frase con cantidad
                                    END as "Cantidad de Producto",
                                    coalesce (tts.tamano,'?') as "Talla", 
                                easy.comuna,
                                easy.estado,
                                easy.subestado,
                                easy.verified,
                                easy.recepcion
                                --drm.nombre_ruta 
                            FROM areati.ti_wms_carga_easy easy
                            left join public.ti_tamano_sku tts on tts.sku = cast(easy.producto as text)
                            --left join quadminds.datos_ruta_manual drm on (drm.cod_pedido = easy.entrega and drm.estado = true )
                            left join public.ti_wms_carga_easy_paso twcep on twcep.entrega = easy.entrega
                            LEFT JOIN (
                                        SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                                        toc.direccion_correcta as direccion, 
                                        toc.comuna_correcta as comuna,
                                        toc.fec_reprogramada as fecha,
                                        toc.observacion,
                                        toc.alerta
                                        FROM rutas.toc_bitacora_mae toc
                                        WHERE toc.alerta = true
                                        ORDER BY toc.guia, toc.created_at desc
                                    ) AS tbm ON easy.entrega=tbm.guia
                                    
                                    
                            WHERE (easy.estado = 0 OR (easy.estado = 2 AND easy.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                            AND easy.estado NOT IN (1, 3)
                            --and easy.entrega not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                            and easy.entrega not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                            --and easy.entrega not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true) 
                        --and easy.fecha_entrega >= '{fecha_inicio}' and easy.fecha_entrega <= '{fecha_fin}'

                        )

            SELECT
                subquery.origen,
                subquery.guia,
                to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
                funcion_resultado."Fecha de Pedido",
                funcion_resultado."Provincia/Estado",
                funcion_resultado."Ciudad",
                --funcion_resultado."Descripción del Producto",
                SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
                funcion_resultado."Cantidad de Producto"::int4,
                ee.descripcion as "Estado",
                se."name" as "Subestado",
                subquery.verified,
                subquery.recepcion,
                subquery.nombre_ruta,
                cast(funcion_resultado."Calle y Número" as VARCHAR ) ,
                funcion_resultado."Talla",
                funcion_resultado."Dirección Textual"
            FROM (
                SELECT DISTINCT ON (easy.entrega)
                    easy.entrega as guia,
                    'Easy' as origen,
                    easy.created_at as fec_ingreso,
                    --easy.fecha_entrega as fec_entrega,
                    coalesce(tbm.fecha,
                            CASE
                                WHEN twcep.fecha_entrega <> easy.fecha_entrega THEN twcep.fecha_entrega
                                ELSE easy.fecha_entrega
                            END
                            ) as fec_entrega,
                    easy.comuna,
                    easy.estado,
                    easy.subestado,
                    easy.verified,
                    easy.recepcion,
                    drm.nombre_ruta
                FROM areati.ti_wms_carga_easy easy
                left join public.ti_wms_carga_easy_paso twcep on twcep.entrega = easy.entrega
                LEFT JOIN (
                            SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                            toc.direccion_correcta as direccion, 
                            toc.comuna_correcta as comuna,
                            toc.fec_reprogramada as fecha,
                            toc.observacion,
                            toc.alerta
                            FROM rutas.toc_bitacora_mae toc
                            WHERE toc.alerta = true
                            ORDER BY toc.guia, toc.created_at desc
                        ) AS tbm ON easy.entrega=tbm.guia
                left join quadminds.datos_ruta_manual drm on (drm.cod_pedido = easy.entrega and drm.estado = true )
                WHERE (easy.estado = 0 OR (easy.estado = 2 AND easy.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                AND easy.estado NOT IN (1, 3)
                and easy.entrega not in (select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                and easy.fecha_entrega >= '{fecha_inicio}' and easy.fecha_entrega <= '{fecha_fin}'
            ) subquery
            
            left join f_aux funcion_resultado on subquery.guia = funcion_resultado.guia
            --JOIN LATERAL areati.busca_ruta_manual_base2(subquery.guia) AS funcion_resultado ON true
            left join areati.estado_entregas ee on subquery.estado = ee.estado 
            left join areati.subestado_entregas se on subquery.subestado = se.code 
            where to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')>='{fecha_inicio}'
            and to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')<='{fecha_fin}'

            """)
            return cur.fetchall()
        
    def pendientes_en_ruta_electrolux(self, fecha_inicio,fecha_fin, offset ):
        with self.conn.cursor() as cur:
            cur.execute(f"""
 with f_aux as (
            select eltx.identificador_contacto AS "Código de Cliente",
                    initcap(eltx.nombre_contacto) AS "Nombre",
                    coalesce(tbm.direccion,
                    CASE 
            WHEN substring(initcap(split_part(eltx.direccion,',',1)) from '^\d') ~ '\d' then substring(initcap(split_part(eltx.direccion,',',1)) from '\d+[\w\s]+\d+')
            WHEN lower(split_part(eltx.direccion,',',1)) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN 
            regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(eltx.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '')
            else coalesce(substring(initcap(split_part(eltx.direccion,',',1)) from '^[^0-9]*[0-9]+'),eltx.direccion)
                    end) as "Calle y Número",
            coalesce(tbm.direccion,eltx.direccion) as "Dirección Textual",
            coalesce(tbm.comuna, case
            when unaccent(lower(eltx.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
            (select oc.comuna_name from public.op_comunas oc 
            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
            where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(eltx.comuna))
                )
                    )
                    else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                where unaccent(lower(eltx.comuna)) = unaccent(lower(oc2.comuna_name))
                )
            end) as "Ciudad",
            case
                        when unaccent(lower(eltx.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select opr.region_name  from public.op_regiones opr 
                        where opr.id_region = (select oc.id_region from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                            where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(eltx.comuna))
                            )	
                        ))
                        else(select opr.region_name  from public.op_regiones opr 
                        where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                        where unaccent(lower(eltx.comuna)) = unaccent(lower(oc2.comuna_name))
                        ))
            end as "Provincia/Estado",
                    CAST (eltx.numero_guia AS varchar) AS "Código de Pedido",
                    coalesce(tbm.fecha,
                    case
                        when (select fecha_siguiente from areati.obtener_dias_habiles(to_char(eltx.created_at + interval '1 day','yyyymmdd'))) <> eltx.fecha_min_entrega
                        then (select fecha_siguiente from areati.obtener_dias_habiles(to_char(eltx.created_at + interval '1 day','yyyymmdd')))
                        else eltx.fecha_min_entrega
                    end) AS "Fecha de Pedido",
                    CAST (eltx.numero_guia AS varchar) AS "Código de Producto",
                    '(Electrolux) ' || REPLACE(eltx.nombre_item, ',', '') AS "Descripción del Producto",
                    cast(eltx.cantidad as numeric) AS "Cantidad de Producto",
                    coalesce (tts.tamano,'?') as "Talla"	
            from areati.ti_wms_carga_electrolux eltx
            left join public.ti_tamano_sku tts on tts.sku = cast(eltx.codigo_item as text)
            left join areati.estado_entregas ee on ee.estado=eltx.estado
            --left join beetrack.ruta_transyanez rb on (eltx.numero_guia=rb.guia and rb.created_at::date = current_date)
            LEFT JOIN (
                SELECT DISTINCT ON (guia) guia as guia, 
                direccion_correcta as direccion, 
                comuna_correcta as comuna,
                fec_reprogramada as fecha,
                observacion,
                alerta
                FROM rutas.toc_bitacora_mae
                WHERE alerta = true
                ORDER BY guia, created_at desc
            ) AS tbm ON eltx.numero_guia=tbm.guia
                WHERE (eltx.estado = 0 OR (eltx.estado = 2 AND eltx.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                AND eltx.estado NOT IN (1, 3)
                and eltx.numero_guia not in (select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                and eltx.fecha_min_entrega >= '{fecha_inicio}' and eltx.fecha_min_entrega <= '{fecha_fin}'
            )

            SELECT
                subquery.origen,
                subquery.guia,
                to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
                funcion_resultado."Fecha de Pedido",
                funcion_resultado."Provincia/Estado",
                funcion_resultado."Ciudad",
                SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
                funcion_resultado."Cantidad de Producto"::int4,
                ee.descripcion as "Estado",
                se."name" as "Subestado",
                subquery.verified,
                subquery.recepcion,
                subquery.nombre_ruta,
                funcion_resultado."Calle y Número",
                funcion_resultado."Talla",
                funcion_resultado."Dirección Textual"
            FROM (
                select distinct on (eltx.numero_guia)
                    eltx.numero_guia as guia,
                    'Electrolux' as origen,
                    eltx.created_at as fec_ingreso,
                    eltx.comuna as comuna,
                    eltx.estado,
                    eltx.subestado, 
                    eltx.verified,
                    eltx.recepcion,
                    drm.nombre_ruta
                from areati.ti_wms_carga_electrolux eltx
                    left join quadminds.datos_ruta_manual drm on (drm.cod_pedido = eltx.numero_guia and drm.estado = true )
                    WHERE (eltx.estado = 0 OR (eltx.estado = 2 AND eltx.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND eltx.estado NOT IN (1, 3)
                    and eltx.numero_guia not in (select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                    and eltx.fecha_min_entrega >= '{fecha_inicio}' and eltx.fecha_min_entrega <= '{fecha_fin}'
                    
                ) subquery
                left join f_aux funcion_resultado on subquery.guia = funcion_resultado."Código de Pedido"
                left join areati.estado_entregas ee on subquery.estado = ee.estado 
                left join areati.subestado_entregas se on subquery.subestado = se.code 
                where to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')>='{fecha_inicio}'
                and to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')<='{fecha_fin}'

            """)
            return cur.fetchall()
        
    def pendientes_en_ruta_easy_opl(self, fecha_inicio,fecha_fin, offset ):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            ---- pendientes tiendas Easy
            with f_aux as (
                select  easygo.rut_cliente AS "Código de Cliente",
                    initcap(easygo.nombre_cliente) AS "Nombre",
                    coalesce(tbm.direccion,
                    CASE 
                        WHEN substring(easygo.direc_despacho from '^\d') ~ '\d' then substring(initcap(easygo.direc_despacho) from '\d+[\w\s]+\d+')
                        WHEN lower(easygo.direc_despacho) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                        regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easygo.direc_despacho,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                        else coalesce(substring(initcap(easygo.direc_despacho) from '^[^0-9]*[0-9]+'),initcap(easygo.direc_despacho))
                    end) as "Calle y Número",
                    coalesce(tbm.direccion,easygo.direc_despacho) as "Dirección Textual",
                    coalesce(tbm.comuna,
                    case
                    when unaccent(lower(easygo.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                    (select oc.comuna_name from public.op_comunas oc 
                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easygo.comuna_despacho))
                    )
                        )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                    where unaccent(lower(easygo.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                    )
                    end) as "Ciudad",
                    case
                        when unaccent(lower(easygo.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select opr.region_name  from public.op_regiones opr 
                        where opr.id_region = (select oc.id_region from public.op_comunas oc 
                            where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                            where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(easygo.comuna_despacho))
                            )	
                        ))
                        else(select opr.region_name  from public.op_regiones opr 
                        where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                        where unaccent(lower(easygo.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                        ))
                    end as "Provincia/Estado",
                    '' AS "Latitud",
                    '' AS "Longitud",
                    coalesce(easygo.fono_cliente ,'0') AS "Teléfono con código de país",
                    lower(easygo.correo_cliente) AS "Email",
                    CAST (easygo.suborden AS varchar) AS "Código de Pedido",
                    coalesce(tbm.fecha,easygo.fec_compromiso) AS "Fecha de Pedido",
                    easygo.fec_compromiso as "Fecha Original Pedido", 						-- [H-001]
                    'E' AS "Operación E/R",
                    coalesce (tts.tamano,'?') as "Talla",	
                    easygo.id_entrega AS "Código de Producto",
                    '(Easy OPL) ' || coalesce(REPLACE(easygo.descripcion, ',', ''),'') AS "Descripción del Producto",
                    cast(easygo.unidades as numeric) AS "Cantidad de Producto"                                     -- Alertado por el Sistema
            from areati.ti_carga_easy_go_opl easygo
            left join ti_comuna_region tcr on
                translate(lower(easygo.comuna_despacho),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
            left join public.ti_tamano_sku tts on tts.sku = cast(easygo.codigo_sku as text)
            left join areati.estado_entregas ee on ee.estado=easygo.estado
            --left join quadminds.ti_respuesta_beetrack rb on easygo.suborden=rb.guia
           -- left join beetrack.ruta_transyanez rb on (easygo.suborden=rb.guia and rb.created_at::date = current_date)
            LEFT JOIN (
                SELECT DISTINCT ON (guia) guia as guia, 
                direccion_correcta as direccion, 
                comuna_correcta as comuna,
                fec_reprogramada as fecha,
                observacion,
                alerta
                FROM rutas.toc_bitacora_mae
                WHERE alerta = true
                ORDER BY guia, created_at desc
            ) AS tbm ON easygo.suborden=tbm.guia
                WHERE (easygo.estado = 0 OR (easygo.estado = 2 AND easygo.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                AND easygo.estado NOT IN (1, 3)
                and easygo.suborden not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                and easygo.fec_compromiso >= '{fecha_inicio}' and easygo.fec_compromiso <= '{fecha_fin}'

            )
            SELECT
                subquery.origen,
                subquery.guia,
                to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
                funcion_resultado."Fecha de Pedido",
                funcion_resultado."Provincia/Estado",
                funcion_resultado."Ciudad",
                --funcion_resultado."Descripción del Producto",
                SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
                funcion_resultado."Cantidad de Producto"::int4,
                ee.descripcion as "Estado",
                se."name" as "Subestado",
                subquery.verified,
                subquery.recepcion,
                subquery.nombre_ruta,
                funcion_resultado."Calle y Número",
                funcion_resultado."Talla",
                funcion_resultado."Dirección Textual"
            FROM (
                select distinct on (opl.suborden)
                    opl.suborden as guia,
                    'Tienda Easy' as origen,
                    opl.created_at as fec_ingreso,
                    coalesce(tbm.fecha,opl.fec_compromiso) as fec_entrega,
                    opl.comuna_despacho as comuna,
                    opl.estado,
                    opl.subestado, 
                    opl.verified,
                    opl.recepcion,
                    drm.nombre_ruta
                from areati.ti_carga_easy_go_opl opl
                LEFT JOIN (
                            SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                            toc.direccion_correcta as direccion, 
                            toc.comuna_correcta as comuna,
                            toc.fec_reprogramada as fecha,
                            toc.observacion,
                            toc.alerta
                            FROM rutas.toc_bitacora_mae toc
                            WHERE toc.alerta = true
                            ORDER BY toc.guia, toc.created_at desc
                        ) AS tbm ON opl.suborden=tbm.guia
                left join quadminds.datos_ruta_manual drm on (drm.cod_pedido = opl.suborden and drm.estado = true )
                WHERE (opl.estado = 0 OR (opl.estado = 2 AND opl.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                AND opl.estado NOT IN (1, 3)
                and opl.suborden not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
                and opl.fec_compromiso >= '{fecha_inicio}' and opl.fec_compromiso <= '{fecha_fin}'
                ------------------------------------------------------------
                --limit 100 offset cuenta
            ) subquery
            left join f_aux funcion_resultado on subquery.guia = funcion_resultado."Código de Pedido"
            --JOIN LATERAL areati.busca_ruta_manual_base2(subquery.guia) AS funcion_resultado ON true
            left join areati.estado_entregas ee on subquery.estado = ee.estado 
            left join areati.subestado_entregas se on subquery.subestado = se.code 
            where to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')>='{fecha_inicio}'
            and to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')<='{fecha_fin}'

            """)
            return cur.fetchall()

    def pendientes_en_ruta_retiro_tienda(self, fecha_inicio,fecha_fin, offset ):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            ---- pendientes tiendas Easy
            SELECT
                subquery.origen,
                subquery.guia,
                to_date(to_char(subquery.fec_ingreso,'yyyy-mm-dd'),'yyyy-mm-dd') as "Fecha Ingreso",
                funcion_resultado."Fecha de Pedido",
                funcion_resultado."Provincia/Estado",
                funcion_resultado."Ciudad",
                --funcion_resultado."Descripción del Producto",
                SUBSTRING(funcion_resultado."Descripción del Producto" FROM POSITION(') ' IN funcion_resultado."Descripción del Producto") + 2) as "Descripción del Producto",
                funcion_resultado."Cantidad de Producto"::int4,
                ee.descripcion as "Estado",
                se."name" as "Subestado",
                subquery.verified,
                subquery.recepcion,
                subquery.nombre_ruta,
                funcion_resultado."Calle y Número",
                funcion_resultado."Talla",
                funcion_resultado."Dirección Textual"
            FROM (
                select distinct on (rtcl.cod_pedido)
                rtcl.cod_pedido as guia,
                'Envio/Retiro' as origen,
                rtcl.created_at as fec_ingreso,
                coalesce(tbm.fecha,rtcl.fecha_pedido) as fec_entrega,
                rtcl.comuna as comuna,
                rtcl.estado,
                rtcl.subestado, 
                rtcl.verified ,
                rtcl.verified as "recepcion",
                drm.nombre_ruta
            from areati.ti_retiro_cliente rtcl
            LEFT JOIN (
                        SELECT DISTINCT ON (toc.guia) toc.guia as guia, 
                        toc.direccion_correcta as direccion, 
                        toc.comuna_correcta as comuna,
                        toc.fec_reprogramada as fecha,
                        toc.observacion,
                        toc.alerta
                        FROM rutas.toc_bitacora_mae toc
                        WHERE toc.alerta = true
                        ORDER BY toc.guia, toc.created_at desc
                    ) AS tbm ON rtcl.cod_pedido=tbm.guia
            left join quadminds.datos_ruta_manual drm on (drm.cod_pedido = rtcl.cod_pedido and drm.estado = true )
            WHERE (rtcl.estado = 0 OR (rtcl.estado = 2 AND rtcl.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
            AND rtcl.estado NOT IN (1, 3)
            and rtcl.cod_pedido not in(select rt.guia from beetrack.ruta_transyanez rt where rt.created_at::date = current_date)
            and rtcl.fecha_pedido >= '{fecha_inicio}' and rtcl.fecha_pedido <= '{fecha_fin}'

            ) subquery
            JOIN LATERAL areati.busca_ruta_manual_base2(subquery.guia) AS funcion_resultado ON true
            left join areati.estado_entregas ee on subquery.estado = ee.estado 
            left join areati.subestado_entregas se on subquery.subestado = se.code 
            where to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')>='{fecha_inicio}'
            and to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')<='{fecha_fin}'

            """)
            return cur.fetchall()


    ###Reportes de rutas mensual
    def get_reportes_rutas_mes(self,mes):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from quadminds.listar_rutas_mensual('{mes}')
            where total_rutas <> 0
                        """)
            return cur.fetchall()
        

    def get_reportes_rutas_diario(self,dia):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from quadminds.reporte_rutas_diario('{dia}');
                        """)
            return cur.fetchall()
        


    def obtener_subestados_entrega(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT parent_code, "name", code
                FROM areati.subestado_entregas
                where code not in(90, 91, 100, 101, 102, 103, 104);              
                          """)
            
            return cur.fetchall()
        

    def obtener_subestados_entrega_log_inversa(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT parent_code, "name", code 
                FROM areati.subestado_entregas
                where habilitado_li = true;   
                          """)
            
            return cur.fetchall()
        
    ### Logistica Inversa
        

    def recuperar_bodega_virtual(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                select * from log_inversa.recuperar_bodega_virtual();         
                         """)
            
            return cur.fetchall()
        
       
    def reingresa_producto_a_operacion(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
                select * from log_inversa.reingresa_producto_operacion(%(Id_user)s, %(Ids_user)s, %(Cliente)s,%(Codigo_pedido)s,%(Lat)s,%(Long)s,%(Ingreso)s);
                         """, data)
            
            return cur.fetchall()
        
    def obtener_estados_entrega(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT estado, descripcion
                FROM areati.estado_entregas
                where estado <> 0 and estado <> 1;           
                         """)
            
            return cur.fetchall()
        

    def obtener_rutas_productos(self,cod_producto):
        with self.conn.cursor() as cur:
            cur.execute(f"""
               select * from rutas.obtiene_ruta_producto('{cod_producto}');      
                         """)
            
            return cur.fetchall()
        

    def obtener_rutas_productos_por_ruta(self,cod_producto):
        with self.conn.cursor() as cur:
            cur.execute(f"""
               -- select * from rutas.obtiene_ruta_producto('{cod_producto}');      
               select	    rt.identificador_ruta as ruta_beetrack,
                            r.nombre_ruta_ty as ruta_transyanez,
                            rt.identificador as patente,
                            initcap(rt.usuario_movil) as driver
                from beetrack.ruta_transyanez rt 
                left join beetrack.route r on r.route = rt.identificador_ruta 
                where rt.guia = '{cod_producto}' 
                or rt.guia = (select aux.entrega from areati.ti_wms_carga_easy aux where aux.carton = '{cod_producto}')
                or r.nombre_ruta_ty = '{cod_producto}'
                order by rt.created_at desc 
                limit 1;       
                         """)
            
            return cur.fetchall()
        
    def pendientes_log_inversa(self,fecha):
        with self.conn.cursor() as cur:
            cur.execute(f"""
               select * from log_inversa.pendientes_dia_li('{fecha}');      
                         """)
            
            return cur.fetchall()
    
        
    def get_lista_productos_ruta(self,nombre_ruta,separador):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select 	lr.fecha_ruta,
                lr.cod_cliente,
                lr.nombre,
                lr.ciudad,
                lr.cod_pedido,
                lr.cod_producto,
                lr.sku,
                lr.fecha_pedido,
                lr.desc_producto,
                lr.cant_producto,
                lr.notas,
                substring(lr.concatenated_data from length(lr.concatenated_data) - position('@' in reverse(lr.concatenated_data)) + 2) as estado
                from rutas.listar_ruta_edicion('{nombre_ruta}') lr          
                         """)
            
            return cur.fetchall()
        

    def get_estados_productos_ruta(self,cod_pedido):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select estado,subestado from areati.ti_carga_easy_go_opl 
                WHERE suborden= '{cod_pedido}'
                union all
                select estado,subestado from areati.ti_wms_carga_easy
                where entrega= '{cod_pedido}'
                union all
                select estado,subestado from areati.ti_wms_carga_electrolux
                where numero_guia = '{cod_pedido}'
                union all
                select estado,subestado from areati.ti_wms_carga_sportex
                WHERE id_sportex = '{cod_pedido}'
                union all
                select estado,subestado from areati.ti_retiro_cliente
                WHERE cod_pedido = '{cod_pedido}'
                limit 1;
                         """)
            
            return cur.fetchall()
        

    def inser_bitacora_log_inversa(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO log_inversa.bitacora_general
                (id_usuario, ids_usuario, estado_inicial, subestado_inicial, estado_final, subestado_final, link, observacion, latitud, longitud, origen, cliente, cod_pedido, cod_producto)
                VALUES(%(Id_user)s, %(Ids_user)s, %(Estado_inicial)s, %(Subestado_inicial)s, %(Estado_final)s,%(Subestado_final)s, %(Link)s, %(Observacion)s, %(Latitud)s, %(Longitud)s, %(Origen_registro)s, %(Origen)s, %(Codigo_pedido)s, %(Codigo_producto)s);     
               
                 """,data)
            self.conn.commit()
        

    def update_estados_pendientes(self,estado, subestado,cod_pedido):

        if estado is None :
            estado = "null"

        if subestado is None :
            subestado = "null"

        sql_queries = [
            f"UPDATE areati.ti_carga_easy_go_opl SET estado = {estado} ,subestado = {subestado} WHERE suborden= '{cod_pedido}'",
            f"UPDATE areati.ti_wms_carga_easy SET estado = {estado} ,subestado = {subestado} WHERE entrega= '{cod_pedido}'",
            f"UPDATE areati.ti_wms_carga_electrolux SET estado = {estado} ,subestado = {subestado} WHERE numero_guia = '{cod_pedido}'",
            f"UPDATE areati.ti_wms_carga_sportex SET estado = {estado} ,subestado = {subestado} WHERE id_sportex = '{cod_pedido}'",
            f"UPDATE areati.ti_retiro_cliente SET estado = {estado} ,subestado = {subestado} WHERE cod_pedido = '{cod_pedido}'",
        ]

        updates = []
        try:
            for query in sql_queries:
                with self.conn.cursor() as cur:
                    cur.execute(query)
                    # print(cur.rowcount)
                    updates.append(cur.rowcount)
                self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error durante la actualización:", error)
        finally:
            # pass
            return updates


    def get_bitacora_log_inversa_tracking(self,cod_producto):
        with self.conn.cursor() as cur:
            cur.execute(f"""
               select 	to_char(created_at,'dd-mm-yyyy hh24:mi') as diahora,
                        cliente,
                        cod_producto,
                        ids_usuario,
                        origen,
                        observacion
                from log_inversa.bitacora_general bg 
                where cod_pedido = '{cod_producto}' or cod_producto = '{cod_producto}'
                order by created_at  desc 
                         """)
            
            return cur.fetchall()
        
    ##### TIMELINE PRODUCTOS SEGURIDAD
        
    def hoja_vida_producto(self,cod_producto):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select * from rutas.genera_hojavida_producto('{cod_producto}');
                         """)
            
            return cur.fetchall()

    
    def prueba_alv_elux(self,cod_producto):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                
                         """)
            
            return cur.fetchall()
        

    def recalcular_posicion_ruta(self,nombre_ruta : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select * from quadminds.recalcular_posicion_ruta('{nombre_ruta}')
                         """)
            return cur.fetchall()

    #### DEFONTANA RSV
        
    def revisar_datos_folio(self,folios : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT numero_folio
                from rsv.defontana_venta
                where numero_folio in ({folios})
                         """)
            lista_tuplas = cur.fetchall()
            lista = [str(elem[0]) for elem in lista_tuplas]
            return lista
        
    def insert_venta_defontana(self,data):

        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rsv.defontana_venta
                (tipo_documento, numero_folio, fecha_emision, fecha_creacion, fecha_expiracion, rut_cliente, direccion, condicion_pago, id_vendedor, id_tienda, giro, ciudad, distrito)
                VALUES(%(documentType)s, %(firstFolio)s, %(emissionDate)s, %(dateTime)s, %(expirationDate)s,%(clientFile)s, %(contactIndex)s, %(paymentCondition)s, %(sellerFileId)s, %(shopId)s, %(giro)s, %(city)s, %(district)s);     
               
                 """,data)
            self.conn.commit()

    def insert_detalle_venta_defontana(self,data, numero_folio):
        data['firstFolio'] = numero_folio
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rsv.defontana_detalle_venta
                (linea_detalle, tipo_detalle, codigo, cantidad, precio, exenta, comentario, total, numero_folio)
                VALUES(%(detailLine)s, %(type)s, %(code)s, %(count)s, %(price)s,
                        %(isExempt)s, %(comment)s, %(total)s, %(firstFolio)s );     
               
                 """,data)
            self.conn.commit()
        
    ##### no entregados total RODRIGO
            
    def read_no_entregados_total(self,fecha,tienda,region):

        data = {
            'fecha': fecha,
            'tienda': tienda,
            'region': region
        }
        with self.conn.cursor() as cur:
            cur.execute("""
                select * from rutas.no_entregados_total(%(fecha)s,%(tienda)s,%(region)s);
                         """, data)
            
            return cur.fetchall()
        
    def read_eficiencia_conductor(self,fecha,tienda,region):
        data = {
            'fecha': fecha,
            'tienda': tienda,
            'region': region
        }
        with self.conn.cursor() as cur:
            cur.execute("""        
                ---EFICIECIA CONDUCTOR
                select 	driver,
                        patente,
                        total,
                        entregados,
                        no_entregado,
                        ee
                FROM rutas.resumen_ns_toc(%(fecha)s,%(tienda)s,%(region)s)
                order by 1 asc;	
                         """, data)
            
            return cur.fetchall()

    def read_media_eficiencia_conductor(self,fecha,tienda,region):
        data = {
            'fecha': fecha,
            'tienda': tienda,
            'region': region
        }
        with self.conn.cursor() as cur:
            cur.execute("""        
                ---MEDIA EFICENCIA CONDUCTOR
                SELECT 
                    SUM(total) AS suma,
                    SUM(entregados) AS t_ent,
                    SUM(no_entregado) AS n_ent,
                    round(AVG(ee),2) AS efectividad_entrega
                FROM rutas.resumen_ns_toc(%(fecha)s,%(tienda)s,%(region)s)
                         """, data)
            
            return cur.fetchone()

    #### Despacho Ruta - NS F_Compromiso

    def buscar_productos_por_4_digitos(self,codigo_producto : str):
        with self.conn.cursor() as cur:
            cur.execute(recuperar_query(codigo_producto))
            return cur.fetchall()

    def revisar_nivel_servicio_fec_real(self,fecha : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select 
                    ns.cliente,
                    coalesce(ns.compromiso_real, 0),
                    coalesce(ns.entregados,0),
                    coalesce(ns.anulados,0),
                    coalesce(ns.nivel_servicio,0)
                from rutas.nivel_servicio_fec_real('{fecha}') as ns;
                         """)
            return cur.fetchall()
        

    def revisar_nivel_servicio_fec_real_easy(self,fecha : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
               -- select 	'Easy CD' as cliente,
                  --      total_registros	as compromiso_real,
                   --     registros_con_entrega_real as entregados,
                    --    total_anulados as anulados,
                     --   porcentaje_entregas_real as nivel_servicio
               -- from rutas.calcular_ns_easy_fecha('{fecha}')
               --------------
                WITH fecha_llegada_ruta as  (
                select distinct on(rt.guia) rt.fecha_llegada::date,
                    rt.guia,
                    rt.cliente 
                FROM beetrack.ruta_transyanez rt
                where (rt.cliente = 'Easy' or rt.cliente = '')
                AND LOWER(rt.estado) IN ('entregado', 'retirado')
                ORDER BY rt.guia,rt.created_at DESC
                ),
                conteo_easy AS (
                            select
                                twce.entrega,
                                twce.fecha_entrega,
                                flr.fecha_llegada::date AS fec_entrega_real,
                                flr.cliente ,
                                CASE
                                    WHEN ((twce.estado = 2 AND twce.subestado IN (7, 10, 12, 19, 43, 44, 50, 51, 70, 80)) OR twce.estado IN (3)) THEN 1
                                    ELSE 0
                                END AS anulado
                            FROM
                                areati.ti_wms_carga_easy twce
                            left join fecha_llegada_ruta flr on flr.guia = twce.entrega and 
                            flr.fecha_llegada::date <= twce.fecha_entrega::date
                            WHERE
                                twce.fecha_entrega::date = '{fecha}'::date -- Fecha Parametro
                        )

                        select 	'Easy CD' as cliente,
                            COUNT(*) AS total_registros,
                            COUNT(fec_entrega_real) AS registros_con_entrega_real,
                            SUM(anulado) AS total_anulados
                            --SUM(anulado) AS total_anulados,
                            --ROUND(COUNT(fec_entrega_real) * 100.0 / NULLIF((COUNT(*) - SUM(anulado)), 0), 2) AS porcentaje_entregas_real
                        FROM
                            conteo_easy;
                         """)
            return cur.fetchall()
        

    def revisar_nivel_servicio_fec_real_easy_opl(self,fecha : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            --select 	'Easy OPL' as cliente,
                   -- total_registros	as compromiso_real,
                   -- registros_con_entrega_real as entregados,
                   -- total_anulados as anulados,
                   -- porcentaje_entregas_real as nivel_servicio
           -- from rutas.calcular_ns_easyopl_fecha('{fecha}')

            WITH conteo_easyOPL AS (
        SELECT
            twce.suborden,
            twce.fec_compromiso,
            (SELECT rt.fecha_llegada::date
             FROM beetrack.ruta_transyanez rt
             WHERE rt.guia = twce.suborden
               AND LOWER(rt.estado) IN ('entregado', 'retirado')
               AND rt.fecha_llegada::date <= twce.fec_compromiso::date
             ORDER BY rt.created_at DESC
             LIMIT 1) AS fec_entrega_real,
            CASE
                WHEN ((twce.estado = 2 AND twce.subestado IN (7, 10, 12, 19, 43, 44, 50, 51, 70, 80)) OR twce.estado IN (3)) THEN 1
                ELSE 0
            END AS anulado
        FROM
            areati.ti_carga_easy_go_opl twce
        WHERE
            twce.fec_compromiso::date = '{fecha}'::date -- Fecha Parametro
    )
    select 	'Easy OPL' as cliente,
        COUNT(*) AS total_registros,
        COUNT(fec_entrega_real) AS registros_con_entrega_real,
        SUM(anulado) AS total_anulados
        --ROUND(COUNT(fec_entrega_real) * 100.0 / NULLIF((COUNT(*) - SUM(anulado)), 0), 2) AS porcentaje_entregas_real
    FROM
        conteo_easyOPL;    
       

                         """)
            return cur.fetchall()
        
    def revisar_nivel_servicio_fec_real_elux(self,fecha : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select 	'Electrolux' as cliente,
                    total_registros	as compromiso_real,
                    registros_con_entrega_real as entregados,
                    coalesce(total_anulados,0) as anulados,
                    coalesce(porcentaje_entregas_real,0) as nivel_servicio
            from rutas.calcular_ns_electrolux_fecha('{fecha}')
            union all
            select 	'Envio/Retiros' as cliente,
                    total_registros	as compromiso_real,
                    registros_con_entrega_real as entregados,
                    coalesce(total_anulados,0) as anulados,
                    coalesce(porcentaje_entregas_real,0) as nivel_servicio
            from rutas.calcular_ns_retirocliente_fecha('{fecha}')

                         """)
            return cur.fetchall()
    
    def revisar_nivel_servicio_fec_real_promedio(self,fecha : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT round(AVG(nivel_servicio),2) AS promedio
                FROM rutas.nivel_servicio_fec_real('{fecha}');
                         """)
            return cur.fetchone()

    ###Diferencias fechas Easy
    def obtener_dif_fechas_easy_excel(self,fecha_inicio : str,fecha_fin : str, offset : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                ---funcion con offset
                --select * from areati.reporte_fechas_easy_desfase('{fecha_inicio}','{fecha_fin}',{offset});
                --query sin limite
                select	subquery.cliente,
			subquery.ingreso_sistema,
			subquery.fecha_compromiso,
			subquery.ultima_actualizacion,
			CASE 
		        WHEN subquery.ultima_actualizacion IS NOT NULL 
		        	THEN to_date(subquery.ultima_actualizacion,'dd/mm/yyyy hh24:mi') - to_date(subquery.ingreso_sistema,'dd/mm/yyyy hh24:mi')
		        ELSE NULL
	    	END AS dias_ejecucion,
			subquery.cod_pedido,
			subquery.id_entrega,
			subquery.direccion,
			subquery.comuna,
			subquery.descripcion,
			subquery.unidades,
			subquery.estado,
			subquery.subestado
	from (
		select 	'Easy OPL' as cliente,
				to_char(tcego.created_at,'dd/mm/yyyy hh24:mi') as ingreso_sistema,
				to_char(tcego.fec_compromiso,'dd/mm/yyyy') as fecha_compromiso,
				(	select to_char(rt.fecha_llegada,'dd/mm/yyyy hh24:mi') 
					from beetrack.ruta_transyanez rt 
					where rt.guia=tcego.suborden 
					limit 1) as ultima_actualizacion,
				tcego.suborden as cod_pedido,
				tcego.id_entrega::text as id_entrega,
				tcego.direc_despacho as direccion,
				tcego.comuna_despacho as comuna,
				tcego.descripcion as descripcion,
				tcego.unidades::text as unidades,
				initcap(ee.descripcion) as estado,
				se."name" as subestado
		from areati.ti_carga_easy_go_opl tcego
		left join areati.estado_entregas ee on ee.estado=tcego.estado
		left join areati.subestado_entregas se on (se.parent_code=tcego.estado and se.code=tcego.subestado)
		where tcego.created_at::date >= tcego.fec_compromiso 
		and tcego.created_at::date >= '{fecha_inicio}'::date and tcego.created_at::date<= '{fecha_fin}'::date
		union all 
		select 	'Easy CD' as cliente,
				to_char(twce.created_at,'dd/mm/yyyy hh24:mi') as ingreso_sistema,
				to_char(twce.fecha_entrega,'dd/mm/yyyy') as fecha_compromiso,
				(	select to_char(rt.fecha_llegada,'dd/mm/yyyy hh24:mi') 
					from beetrack.ruta_transyanez rt 
					where rt.guia=twce.entrega 
					limit 1) as ultima_actualizacion,
				twce.entrega as cod_pedido,
				twce.carton as id_entrega,
				twce.direccion as direccion,
				twce.comuna as comuna,
				twce.descripcion as descripcion,
				twce.cant as unidades,
				initcap(ee.descripcion) as estado,
				se."name" as subestado
		from areati.ti_wms_carga_easy twce
		left join areati.estado_entregas ee on ee.estado=twce.estado
		left join areati.subestado_entregas se on (se.parent_code=twce.estado and se.code=twce.subestado)
		where twce.created_at::date >= twce.fecha_entrega 
		and twce.created_at::date >= '{fecha_inicio}'::date and twce.created_at::date<= '{fecha_fin}'::date
		order by cliente, ingreso_sistema asc
	) subquery
                         """)
            return cur.fetchall()


    ###Productos Ingresados
    def obtener_Productos_ingresados_excel(self,fecha_inicio : str,fecha_fin : str, offset : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                ---funcion con offset
                --select * from areati.reporte_productos_ingresado('{fecha_inicio}','{fecha_fin}',{offset});
                --query sin limite
                ----------------------------------------------------------------------------
                -- (1) Recupera Easy CD
                ----------------------------------------------------------------------------
                select  to_char(twce.created_at,'dd/mm/yyyy hh24:mi:ss') as ingreso_sistema,
                        'Easy CD' as cliente,
                        CASE
                            WHEN twce.anden IS NULL THEN easy.anden
                            ELSE twce.anden
                        end as anden,
                        -- twce.anden as anden,
                        twce.entrega as cod_pedido,
                        twce.fecha_entrega as fec_compromiso,
                        twce.carton as cod_producto,
                        twce.producto::text as sku,
                        case
                        when unaccent(lower(twce.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select oc.comuna_name from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twce.comuna))
                                                    )
                                )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(twce.comuna)) = unaccent(lower(oc2.comuna_name))
                                )
                        end as comuna,
                        case
                            when unaccent(lower(twce.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select opr.region_name  from public.op_regiones opr 
                            where opr.id_region = (select oc.id_region from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twce.comuna))
                                )    
                            ))
                            else(select opr.region_name  from public.op_regiones opr 
                            where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                            where unaccent(lower(twce.comuna)) = unaccent(lower(oc2.comuna_name))
                            ))
                        end as region,
                        twce.bultos as cantidad,
                        twce.verified as verificado,
                        twce.recepcion as recepcionado,
                        ee.descripcion as estado,
                        se.name as subestado 
                from areati.ti_wms_carga_easy twce
                left join public.ti_wms_carga_easy_paso easy on  easy.carton = (CASE 
                        WHEN POSITION('-' IN twce.carton) > 0 THEN 
                            substring(twce.carton FROM 1 FOR POSITION('-' IN twce.carton) - 1)
                        ELSE 
                            twce.carton
                        END )
                left join areati.estado_entregas ee on ee.estado = twce.estado
                left join areati.subestado_entregas se on (se.parent_code = twce.estado and se.code = twce.subestado)
                where twce.created_at::date >= '{fecha_inicio}'::date and twce.created_at::date <= '{fecha_fin}'::date
                ----------------------------------------------------------------------------
                -- (2) Recupera Easy OPL
                ----------------------------------------------------------------------------
                union all
                select  to_char(tcego.created_at,'dd/mm/yyyy hh24:mi:ss') as ingreso_sistema,
                        'Easy Tienda' as cliente,
                        null as anden,
                        tcego.suborden as cod_pedido,
                        tcego.fec_compromiso as fec_compromiso,
                        tcego.codigo_ean as cod_producto,
                        tcego.codigo_sku::text as sku,
                        case
                        when unaccent(lower(tcego.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select oc.comuna_name from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(tcego.comuna_despacho))
                                                    )
                                )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(tcego.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                                )
                        end as comuna,
                        case
                            when unaccent(lower(tcego.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select opr.region_name  from public.op_regiones opr 
                            where opr.id_region = (select oc.id_region from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(tcego.comuna_despacho))
                                )    
                            ))
                            else(select opr.region_name  from public.op_regiones opr 
                            where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                            where unaccent(lower(tcego.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                            ))
                        end as region,
                        tcego.unidades as cantidad,
                        tcego.verified as verificado,
                        tcego.recepcion as recepcionado,
                        ee.descripcion as estado,
                        se.name as subestado 
                from areati.ti_carga_easy_go_opl tcego
                left join areati.estado_entregas ee on ee.estado = tcego.estado
                left join areati.subestado_entregas se on (se.parent_code = tcego.estado and se.code = tcego.subestado)
                where tcego.created_at::date >= '{fecha_inicio}'::date and tcego.created_at::date <= '{fecha_fin}'::date
                ----------------------------------------------------------------------------
                -- (3) Recupera Electrolux
                ----------------------------------------------------------------------------
                union all
                select  to_char(eltx.created_at,'dd/mm/yyyy hh24:mi:ss') as ingreso_sistema,
                        'Electrolux' as cliente,
                        eltx.ruta as anden,
                        eltx.numero_guia as cod_pedido,
                        calculo.fecha_siguiente as fec_compromiso,
                        eltx.entrega as cod_producto,
                        eltx.codigo_item::text as sku,
                        case
                        when unaccent(lower(eltx.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select oc.comuna_name from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(eltx.comuna))
                                                    )
                                )
                        else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(eltx.comuna)) = unaccent(lower(oc2.comuna_name))
                                )
                        end as comuna,
                        case
                            when unaccent(lower(eltx.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select opr.region_name  from public.op_regiones opr 
                            where opr.id_region = (select oc.id_region from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(eltx.comuna))
                                )    
                            ))
                            else(select opr.region_name  from public.op_regiones opr 
                            where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                            where unaccent(lower(eltx.comuna)) = unaccent(lower(oc2.comuna_name))
                            ))
                        end as region,
                        eltx.cantidad as cantidad,
                        eltx.verified as verificado,
                        eltx.recepcion as recepcionado,
                        ee.descripcion as estado,
                        se.name as subestado 
                from areati.ti_wms_carga_electrolux eltx
                left join areati.estado_entregas ee on ee.estado = eltx.estado
                left join areati.subestado_entregas se on (se.parent_code = eltx.estado and se.code = eltx.subestado)
                left join (
                            select distinct on (numero_guia)
                            numero_guia, 
                            to_date(to_char(created_at,'yyyy-mm-dd'),'yyyy-mm-dd') as ingreso,
                            fecha_min_entrega,
                            CASE
                            WHEN EXTRACT(ISODOW FROM created_at + INTERVAL '1 day') = 7 THEN to_date(to_char(created_at + INTERVAL '3 days','yyyy-mm-dd'),'yyyy-mm-dd')
                            ELSE to_date(to_char(created_at + INTERVAL '2 days','yyyy-mm-dd'),'yyyy-mm-dd')
                            END AS fecha_siguiente
                            from areati.ti_wms_carga_electrolux   
                        ) as calculo on calculo.numero_guia = eltx.numero_guia
                where eltx.created_at::date >= '{fecha_inicio}'::date and eltx.created_at::date <= '{fecha_fin}'::date
                ----------------------------------------------------------------------------
                -- (4) Recupera ingreso Manual
                ----------------------------------------------------------------------------
                union all	
                select  to_char(trc.created_at,'dd/mm/yyyy hh24:mi:ss') as ingreso_sistema,
                        'Ingreso Manual' as cliente,
                        trc.cliente as anden,
                        trc.cod_pedido as cod_pedido,
                        trc.fecha_pedido as fec_compromiso,
                        trc.envio_asociado as cod_producto,
                        trc.sku::text as sku,
                        trc.comuna as comuna,
                        case
                            when unaccent(lower(trc.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                            (select opr.region_name  from public.op_regiones opr 
                            where opr.id_region = (select oc.id_region from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(trc.comuna))
                                )    
                            ))
                            else(select opr.region_name  from public.op_regiones opr 
                            where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                            where unaccent(lower(trc.comuna)) = unaccent(lower(oc2.comuna_name))
                            ))
                        end as region,
                        trc.bultos as cantidad,
                        trc.verified as verificado,
                        trc.verified as recepcionado,
                        ee.descripcion as estado,
                        se.name as subestado 
                from areati.ti_retiro_cliente trc
                left join areati.estado_entregas ee on ee.estado = trc.estado
                left join areati.subestado_entregas se on (se.parent_code = trc.estado and se.code = trc.subestado)
                where trc.created_at::date >= '{fecha_inicio}'::date and trc.created_at::date <= '{fecha_fin}'::date
                ----------------------------------------------------------------------------
                order by ingreso_sistema asc

                         """)
            return cur.fetchall()

    ### reporte telefono truncado
    def obtener_telefonos_truncados_excel(self,fecha_inicio : str,fecha_fin : str, offset : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            ---funcion con offset
            --select * from areati.reporte_telefonos_truncados('{fecha_inicio}','{fecha_fin}',{offset});
            --query sin limite
        ----------------------------------------------------------------------------
        -- (1) Recupera Easy CD
        ----------------------------------------------------------------------------
        select  to_char(twce.created_at,'dd/mm/yyyy hh24:mi:ss') as ingreso_sistema,
                'Easy CD' as cliente,
                twce.telefono as telefono,
                -- twce.anden as anden,
                twce.entrega as cod_pedido,
                twce.fecha_entrega as fec_compromiso,
                twce.carton as cod_producto,
                twce.producto::text as sku,
                initcap(twce.nombre) as nombre,
                twce.direccion as direccion_real,
                case
                when unaccent(lower(twce.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                        (select oc.comuna_name from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twce.comuna))
                                            )
                        )
                else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                        where unaccent(lower(twce.comuna)) = unaccent(lower(oc2.comuna_name))
                        )
                end as comuna,
                case
                    when unaccent(lower(twce.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                    (select opr.region_name  from public.op_regiones opr 
                    where opr.id_region = (select oc.id_region from public.op_comunas oc 
                        where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twce.comuna))
                        )    
                    ))
                    else(select opr.region_name  from public.op_regiones opr 
                    where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                    where unaccent(lower(twce.comuna)) = unaccent(lower(oc2.comuna_name))
                    ))
                end as region,
                twce.bultos as cantidad,
                twce.verified as verificado,
                twce.recepcion as recepcionado,
                ee.descripcion as estado,
                se.name as subestado 
        from areati.ti_wms_carga_easy twce
        left join public.ti_wms_carga_easy_paso easy on  easy.carton = (CASE 
                WHEN POSITION('-' IN twce.carton) > 0 THEN 
                    substring(twce.carton FROM 1 FOR POSITION('-' IN twce.carton) - 1)
                ELSE 
                    twce.carton
                END )
        left join areati.estado_entregas ee on ee.estado = twce.estado
        left join areati.subestado_entregas se on (se.parent_code = twce.estado and se.code = twce.subestado)
        where twce.created_at::date >= '{fecha_inicio}'::date and twce.created_at::date <=  '{fecha_fin}'::date
        --where twce.created_at::date >= '{fecha_inicio}'::date and twce.created_at::date <= '{fecha_fin}'::date
        and CHAR_LENGTH(REGEXP_REPLACE(twce.telefono, '[^0-9]', '', 'g')) <= 7
        order by ingreso_sistema asc

                         """)
            return cur.fetchall()


    ###Regiones
    def obtener_region(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select distinct on (region_beetrack)
                    region_beetrack
                from public.ti_comuna_region tcr
                         """)
            return cur.fetchall()
        
    ###Notificaciones defontana
    def notificacion_defontana_hoy(self,dias7):
        with self.conn.cursor() as cur:
            cur.execute(f"""    
                --SELECT dv.numero_folio, nv.preparado FROM rsv.defontana_venta dv 
                -- left join rsv.nota_venta nv on cast(dv.numero_folio as varchar)  = nv.numero_factura 
                 --where TO_CHAR(dv.fecha_creacion, 'DD-MM-YYYY') = TO_CHAR(CURRENT_DATE, 'DD-MM-YYYY')

                SELECT dv.numero_folio, nv.preparado FROM rsv.defontana_venta dv 
	            left join rsv.nota_venta nv on cast(dv.numero_folio as varchar)  = nv.numero_factura 
	            where dv.fecha_creacion::date >= '{dias7}'::date and dv.fecha_creacion::date <= current_date::date
               --- and nv.preparado = false
 
                        
                         """)
            return cur.fetchall()
        

        ###comuna_por_ruta
    def comuna_por_ruta(self,fecha):
        with self.conn.cursor() as cur:
            cur.execute(f"""    
                select * from rutas.comunas_por_ruta('{fecha}');
                         """)
            return cur.fetchall()
        

    ### seguimiento_transporte
    def seguimiento_transporte(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                 --select * from rutas.seguimiento_transporte(); 
                 -- select * from rutas.seguimiento_transporte_v2();
            select 
           		ruta_beetrack,
           		ppu,
           		region,
           		cliente,
           		carga_total,
           		fecha_compromiso,
           		entregados,
           		entregado_fec_comp,
           		pendientes,
           		no_entregados,
           		obs_total_pedidos
           	from rutas.seguimiento_transporte_v2();
                        
                         """)
            return cur.fetchall()
        

        ###  NS Drivers
    def nivel_servicio_drivers(self,fecha_inicio,fecha_fin):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                 select * from rutas.ns_por_patente('{fecha_inicio}','{fecha_fin}');
                         """)
            return cur.fetchall()
    

    def detalle_pendientes_easy_hoy_v2(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
                        
                ---- V1  select * from areati.detalle_pendientes_easy_hoy()
                SELECT *
                FROM (
                    SELECT DISTINCT ON (eefc.entrega)
                        eefc.cliente,
                        eefc.entrega AS guia,
                        drm.nombre_ruta AS ruta_hela,
                        initcap(drm.calle_numero) AS direccion,
                        drm.ciudad AS ciudad,
                        drm.provincia_estado AS region
                    FROM areati.estatus_entregas_fec_compromiso_easy() AS eefc
                    LEFT JOIN quadminds.datos_ruta_manual drm ON drm.cod_pedido = eefc.entrega
                    WHERE eefc.entrega NOT IN (
                        SELECT rt.guia 
                        FROM beetrack.ruta_transyanez rt
                        WHERE rt.created_at::date = current_date
                    )
                    AND eefc.estado != 1 AND drm.nombre_ruta IS NOT NULL
                    UNION ALL  
                    SELECT DISTINCT ON (eefc.entrega)
                        eefc.cliente,
                        eefc.entrega AS guia,
                        drm.nombre_ruta AS ruta_hela,
                        initcap(brm."Dirección Textual") AS direccion,
                        brm."Ciudad" AS ciudad,
                        brm."Provincia/Estado" AS region
                    FROM areati.estatus_entregas_fec_compromiso_easy() AS eefc
                    LEFT JOIN quadminds.datos_ruta_manual drm ON drm.cod_pedido = eefc.entrega
                    LEFT JOIN LATERAL (
                        SELECT * FROM areati.busca_ruta_manual_base2(eefc.entrega) LIMIT 1
                    ) AS brm ON true
                    WHERE eefc.entrega NOT IN (
                        SELECT rt.guia 
                        FROM beetrack.ruta_transyanez rt
                        WHERE rt.created_at::date = current_date
                    )
                    AND eefc.estado != 1 AND drm.nombre_ruta IS NULL
                ) AS subquery
                ORDER BY subquery.ruta_hela ASC;
            
                         """)
            return cur.fetchall()
        
    def detalle_pendientes_easy_hoy(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            ----- V4 (mejor que antes ) sin left join de busca_ruta_manual_base2 para los sin ruta
               
               
with estatus_entregas_fec_compromiso_easy as (

            SELECT *
                FROM (
                    -- Consulta para 'Easy CD'
                    SELECT DISTINCT ON (twce.entrega)
                        'Easy CD' as cliente,
                        twce.entrega as entrega,
                        case
                                when unaccent(lower(twce.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select oc.comuna_name from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twce.comuna))
                                                                )
                                )
                                else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                        where unaccent(lower(twce.comuna)) = unaccent(lower(oc2.comuna_name))
                                        )
                            end as comuna,
                            case
                                when unaccent(lower(twce.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select opr.region_name  from public.op_regiones opr 
                                where opr.id_region = (select oc.id_region from public.op_comunas oc 
                                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twce.comuna))
                                    )	
                                ))
                                else(select opr.region_name  from public.op_regiones opr 
                                where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                                where unaccent(lower(twce.comuna)) = unaccent(lower(oc2.comuna_name))
                                ))
                            end as region,
                        --	rsb.cant_por_producto,
                        --	rsb.bultos,
                            twce.estado,
                            twce.subestado,
                            coalesce(tbm.direccion_correcta,twce.direccion) as "Dirección Textual"
                    FROM areati.ti_wms_carga_easy twce
                    left join rutas.toc_bitacora_mae tbm on twce.entrega=tbm.guia and  alerta = true
                    --LEFT JOIN LATERAL rutas.recupera_sku_lite(twce.entrega) AS rsb ON true
                    WHERE twce.fecha_entrega = current_date::date
                    AND twce.fecha_entrega > twce.created_at
                    
                    UNION ALL
                    
                    -- Consulta para 'Easy OPL'
                    SELECT DISTINCT ON (tcego.suborden)
                        'Easy OPL' as cliente,
                        tcego.suborden as entrega,
                        case
                                when unaccent(lower(tcego.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select oc.comuna_name from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(tcego.comuna_despacho))
                                )
                                    )
                                    else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(tcego.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                                )
                        end as comuna,
                        case
                                when unaccent(lower(tcego.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select opr.region_name  from public.op_regiones opr 
                                where opr.id_region = (select oc.id_region from public.op_comunas oc 
                                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(tcego.comuna_despacho))
                                    )	
                                ))
                                else(select opr.region_name  from public.op_regiones opr 
                                where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                                where unaccent(lower(tcego.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                                ))
                        end as region,
                        --rsb.cant_por_producto ,
                        --rsb.bultos,
                        tcego.estado,
                        tcego.subestado,
                        coalesce(tbm.direccion_correcta,tcego.direc_despacho) as "Dirección Textual"
                    FROM areati.ti_carga_easy_go_opl tcego 
                    left join rutas.toc_bitacora_mae tbm on tcego.suborden=tbm.guia and  alerta = true
                    --LEFT JOIN LATERAL rutas.recupera_sku_lite(tcego.suborden) AS rsb ON true
                    WHERE tcego.fec_compromiso = current_date::date
                    AND tcego.fec_compromiso > tcego.created_at
                ) AS subquery

            )
            
SELECT *
                FROM (
                    SELECT DISTINCT ON (eefc.entrega)
                        eefc.cliente,
                        eefc.entrega AS guia,
                        drm.nombre_ruta AS ruta_hela,
                        initcap(drm.calle_numero) AS direccion,
                        drm.ciudad AS ciudad,
                        drm.provincia_estado AS region
                    FROM estatus_entregas_fec_compromiso_easy AS eefc
                    LEFT JOIN quadminds.datos_ruta_manual drm ON drm.cod_pedido = eefc.entrega
                    WHERE eefc.entrega NOT IN (
                        SELECT rt.guia 
                        FROM beetrack.ruta_transyanez rt
                        WHERE rt.created_at::date = current_date
                    )
                    AND eefc.estado != 1 AND drm.nombre_ruta IS NOT NULL
                    UNION ALL  
                    SELECT DISTINCT ON (eefc.entrega)
                        eefc.cliente,
                        eefc.entrega AS guia,
                        drm.nombre_ruta AS ruta_hela,
                        initcap(eefc."Dirección Textual") AS direccion,
                        eefc.comuna AS ciudad,
                        eefc.region AS region
                    FROM estatus_entregas_fec_compromiso_easy AS eefc
                    LEFT JOIN quadminds.datos_ruta_manual drm ON drm.cod_pedido = eefc.entrega
                    --LEFT JOIN LATERAL (
                    --    SELECT * FROM areati.busca_ruta_manual_base2(eefc.entrega) LIMIT 1
                    --) AS brm ON true
                    WHERE eefc.entrega NOT IN (
                        SELECT rt.guia 
                        FROM beetrack.ruta_transyanez rt
                        WHERE rt.created_at::date = current_date
                    )
                    AND eefc.estado != 1 AND drm.nombre_ruta IS NULL
                ) AS subquery
                ORDER BY subquery.ruta_hela ASC;

                         """)
            return cur.fetchall()

    def detalle_pendientes_easy_hoy_con_ruta(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
                SELECT *
                FROM (
                    SELECT DISTINCT ON (eefc.entrega)
                        eefc.cliente,
                        eefc.entrega AS guia,
                        drm.nombre_ruta AS ruta_hela,
                        initcap(drm.calle_numero) AS direccion,
                        drm.ciudad AS ciudad,
                        drm.provincia_estado AS region
                    FROM areati.estatus_entregas_fec_compromiso_easy() AS eefc
                    LEFT JOIN quadminds.datos_ruta_manual drm ON drm.cod_pedido = eefc.entrega
                    WHERE eefc.entrega NOT IN (
                        SELECT rt.guia 
                        FROM beetrack.ruta_transyanez rt
                        WHERE rt.created_at::date = current_date
                    )
                    AND eefc.estado != 1 AND drm.nombre_ruta IS NOT NULL
                ) AS subquery
                ORDER BY subquery.ruta_hela ASC;
            
                         """)
            return cur.fetchall()
        
    def detalle_pendientes_easy_hoy_sin_ruta(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
                SELECT *
                FROM (
                    SELECT DISTINCT ON (eefc.entrega)
                        eefc.cliente,
                        eefc.entrega AS guia,
                        drm.nombre_ruta AS ruta_hela,
                        initcap(brm."Dirección Textual") AS direccion,
                        brm."Ciudad" AS ciudad,
                        brm."Provincia/Estado" AS region
                    FROM areati.estatus_entregas_fec_compromiso_easy() AS eefc
                    LEFT JOIN quadminds.datos_ruta_manual drm ON drm.cod_pedido = eefc.entrega
                    LEFT JOIN LATERAL (
                        SELECT * FROM areati.busca_ruta_manual_base2(eefc.entrega) LIMIT 1
                    ) AS brm ON true
                    WHERE eefc.entrega NOT IN (
                        SELECT rt.guia 
                        FROM beetrack.ruta_transyanez rt
                        WHERE rt.created_at::date = current_date
                    )
                    AND eefc.estado != 1 AND drm.nombre_ruta IS NULL
                ) AS subquery
                ORDER BY subquery.ruta_hela ASC;
            
                         """)
            return cur.fetchall()
     
    def detalle_pendientes_easy_por_region(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   

            with f_aux as (
            SELECT *
            FROM (
                SELECT DISTINCT ON (eefc.entrega)
                    eefc.cliente,
                    eefc.entrega AS guia,
                    drm.nombre_ruta AS ruta_hela,
                    initcap(drm.calle_numero) AS direccion,
                    drm.ciudad AS ciudad,
                    drm.provincia_estado AS region
                FROM areati.estatus_entregas_fec_compromiso_easy() AS eefc
                LEFT JOIN quadminds.datos_ruta_manual drm ON drm.cod_pedido = eefc.entrega
                WHERE eefc.entrega NOT IN (
                    SELECT rt.guia 
                    FROM beetrack.ruta_transyanez rt
                    WHERE rt.created_at::date = current_date
                )
                AND eefc.estado != 1 AND drm.nombre_ruta IS NOT null
                UNION ALL 
                SELECT DISTINCT ON (eefc.entrega)
                    eefc.cliente,
                    eefc.entrega AS guia,
                    drm.nombre_ruta AS ruta_hela,
                    initcap(brm."Dirección Textual") AS direccion,
                    brm."Ciudad" AS ciudad,
                    brm."Provincia/Estado" AS region
                FROM areati.estatus_entregas_fec_compromiso_easy() AS eefc
                LEFT JOIN quadminds.datos_ruta_manual drm ON drm.cod_pedido = eefc.entrega
                LEFT JOIN LATERAL (
                    SELECT * FROM areati.busca_ruta_manual_base2(eefc.entrega) LIMIT 1
                ) AS brm ON true
                WHERE eefc.entrega NOT IN (
                    SELECT rt.guia 
                    FROM beetrack.ruta_transyanez rt
                    WHERE rt.created_at::date = current_date
                )
                AND eefc.estado != 1 AND drm.nombre_ruta IS NULL
            ) AS subquery
            ORDER BY subquery.ruta_hela asc
            )

            select region, ciudad as comuna, count(*) as pendientes
            from f_aux
            group by 1,2 
            order by 1 desc

                         """)
            return cur.fetchall()

    def panel_principal_ns_easy(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            with estatus_entregas_fec_compromiso_easy as (

            SELECT *
                FROM (
                    -- Consulta para 'Easy CD'
                    SELECT DISTINCT ON (twce.entrega)
                        'Easy CD' as cliente,
                        twce.entrega as entrega,
                        case
                                when unaccent(lower(twce.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select oc.comuna_name from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twce.comuna))
                                                                )
                                )
                                else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                        where unaccent(lower(twce.comuna)) = unaccent(lower(oc2.comuna_name))
                                        )
                            end as comuna,
                            case
                                when unaccent(lower(twce.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select opr.region_name  from public.op_regiones opr 
                                where opr.id_region = (select oc.id_region from public.op_comunas oc 
                                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twce.comuna))
                                    )	
                                ))
                                else(select opr.region_name  from public.op_regiones opr 
                                where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                                where unaccent(lower(twce.comuna)) = unaccent(lower(oc2.comuna_name))
                                ))
                            end as region,
                        --	rsb.cant_por_producto,
                        --	rsb.bultos,
                            twce.estado,
                            twce.subestado,
                            coalesce(tbm.direccion_correcta,twce.direccion) as "Dirección Textual"
                    FROM areati.ti_wms_carga_easy twce
                    left join rutas.toc_bitacora_mae tbm on twce.entrega=tbm.guia and  alerta = true
                    --LEFT JOIN LATERAL rutas.recupera_sku_lite(twce.entrega) AS rsb ON true
                    WHERE twce.fecha_entrega = current_date::date
                    AND twce.fecha_entrega > twce.created_at
                    
                    UNION ALL
                    
                    -- Consulta para 'Easy OPL'
                    SELECT DISTINCT ON (tcego.suborden)
                        'Easy OPL' as cliente,
                        tcego.suborden as entrega,
                        case
                                when unaccent(lower(tcego.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select oc.comuna_name from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(tcego.comuna_despacho))
                                )
                                    )
                                    else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(tcego.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                                )
                        end as comuna,
                        case
                                when unaccent(lower(tcego.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select opr.region_name  from public.op_regiones opr 
                                where opr.id_region = (select oc.id_region from public.op_comunas oc 
                                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(tcego.comuna_despacho))
                                    )	
                                ))
                                else(select opr.region_name  from public.op_regiones opr 
                                where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                                where unaccent(lower(tcego.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                                ))
                        end as region,
                        --rsb.cant_por_producto ,
                        --rsb.bultos,
                        tcego.estado,
                        tcego.subestado,
                        coalesce(tbm.direccion_correcta,tcego.direc_despacho) as "Dirección Textual"
                    FROM areati.ti_carga_easy_go_opl tcego 
                    left join rutas.toc_bitacora_mae tbm on tcego.suborden=tbm.guia and  alerta = true
                    --LEFT JOIN LATERAL rutas.recupera_sku_lite(tcego.suborden) AS rsb ON true
                    WHERE tcego.fec_compromiso = current_date::date
                    AND tcego.fec_compromiso > tcego.created_at
                ) AS subquery

            )
               

            select ns_easy.total,ns_easy.total_entregados,ns_easy.entregados_hoy,ns_easy.en_ruta,
                ns_easy.sin_ruta_beetrack,ns_easy.anulados,
                CAST(ns_easy.p_entrega AS float),
                CAST(ns_easy.p_noentrega AS float),
                CAST(ns_easy.proyeccion AS float),
                ns_easy.pendientes_en_ruta, 
			   CAST(ns_easy.p_pendientes_en_ruta AS float), 
		       ns_easy.no_entregado,
		       CAST(ns_easy.p_no_entregado AS float), 
		       CAST(ns_easy.p_sin_ruta_beetrack AS float)
                        
            --from areati.panel_principal_ns_easy() as ns_easy;
              from (SELECT 
        subquery.total,
        subquery.total_entregados,
        subquery.entregados_hoy,
        subquery.en_ruta,
        subquery.pendientes_en_ruta,
        ROUND(subquery.pendientes_en_ruta * 100.00 / subquery.total, 2) AS p_pendientes_en_ruta,
        subquery.no_entregado,
        ROUND(subquery.no_entregado * 100.00 / subquery.total, 2) AS p_no_entregado,
        subquery.sin_ruta_beetrack,
        ROUND(subquery.sin_ruta_beetrack * 100.00 / subquery.total, 2) AS p_sin_ruta_beetrack,
        subquery.anulados,
        ROUND(subquery.total_entregados * 100.00 / subquery.total, 2) AS p_entrega,
        ROUND(100 - (subquery.total_entregados * 100.00 / subquery.total), 2) AS p_noentrega,
        ROUND(100.00 - ((subquery.sin_ruta_beetrack + subquery.no_entregado) * 100.00 / subquery.total) , 2) as proyeccion
    FROM (
        SELECT 
            COUNT(eefc.*) AS total,
            COUNT(CASE WHEN eefc.estado = 1 THEN 1 END) AS total_entregados,
            COUNT(CASE WHEN LOWER(rt.estado) = 'entregado' THEN 1 END) AS entregados_hoy,
            (	select count(*)
				from (
				select 	distinct on (rt.guia)
						rt.guia,
						twce.entrega as easy,
						tcego.suborden as opl
				from beetrack.ruta_transyanez rt 
				left join areati.ti_wms_carga_easy twce on twce.entrega = rt.guia
				left join areati.ti_carga_easy_go_opl tcego on tcego.suborden = rt.guia
				where rt.created_at::date = current_date and lower(rt.cliente) like '%easy%'
				and lower(rt.estado) isnull
				and rt.fechaentrega::date = current_date
				and (twce.entrega notnull or tcego.suborden notnull)
				order by rt.guia asc
				) as subquery1) as pendientes_en_ruta,
            COUNT(DISTINCT rt.guia) AS en_ruta,
            COUNT(CASE WHEN eefc.estado != 1 AND rt.guia IS NULL THEN 1 END) AS sin_ruta_beetrack,
            count(case when rt.estado notnull and lower(rt.estado) != 'entregado' then 1 end) as no_entregado,
            COUNT(CASE WHEN (eefc.estado = 2 AND eefc.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80) OR eefc.estado = 3) THEN 1 END) AS anulados
        FROM estatus_entregas_fec_compromiso_easy AS eefc
        LEFT JOIN (
            SELECT DISTINCT ON (guia) guia, estado 
            FROM beetrack.ruta_transyanez 
            WHERE created_at::date = current_date
        ) rt ON rt.guia = eefc.entrega
    ) AS subquery ) as ns_easy

                         """)
            return cur.fetchone()


    def detalle_pendientes_electrolux_hoy(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select * from areati.detalle_pendientes_electrolux_hoy()

                         """)
            return cur.fetchall()  
    def panel_regiones_ns_easy(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            --select region, total_region, entregados,
               -- CAST(ns_region AS float)
            --from areati.panel_regiones_ns_easy();
            with estatus_entregas_fec_compromiso_easy as (

            SELECT *
                FROM (
                    -- Consulta para 'Easy CD'
                    SELECT DISTINCT ON (twce.entrega)
                        'Easy CD' as cliente,
                        twce.entrega as entrega,
                        case
                                when unaccent(lower(twce.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select oc.comuna_name from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                                                        where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twce.comuna))
                                                                )
                                )
                                else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                        where unaccent(lower(twce.comuna)) = unaccent(lower(oc2.comuna_name))
                                        )
                            end as comuna,
                            case
                                when unaccent(lower(twce.comuna)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select opr.region_name  from public.op_regiones opr 
                                where opr.id_region = (select oc.id_region from public.op_comunas oc 
                                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(twce.comuna))
                                    )	
                                ))
                                else(select opr.region_name  from public.op_regiones opr 
                                where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                                where unaccent(lower(twce.comuna)) = unaccent(lower(oc2.comuna_name))
                                ))
                            end as region,
                        --	rsb.cant_por_producto,
                        --	rsb.bultos,
                            twce.estado,
                            twce.subestado
                    FROM areati.ti_wms_carga_easy twce
                    --LEFT JOIN LATERAL rutas.recupera_sku_lite(twce.entrega) AS rsb ON true
                    WHERE twce.fecha_entrega = current_date::date
                    AND twce.fecha_entrega > twce.created_at
                    
                    UNION ALL
                    
                    -- Consulta para 'Easy OPL'
                    SELECT DISTINCT ON (tcego.suborden)
                        'Easy OPL' as cliente,
                        tcego.suborden as entrega,
                        case
                                when unaccent(lower(tcego.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select oc.comuna_name from public.op_comunas oc 
                                where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(tcego.comuna_despacho))
                                )
                                    )
                                    else (select initcap(oc2.comuna_name) from public.op_comunas oc2 
                                where unaccent(lower(tcego.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                                )
                        end as comuna,
                        case
                                when unaccent(lower(tcego.comuna_despacho)) not in (select unaccent(lower(op.comuna_name)) from public.op_comunas op) then
                                (select opr.region_name  from public.op_regiones opr 
                                where opr.id_region = (select oc.id_region from public.op_comunas oc 
                                    where oc.id_comuna = ( select occ.id_comuna  from public.op_corregir_comuna occ 
                                    where unaccent(lower(occ.comuna_corregir)) = unaccent(lower(tcego.comuna_despacho))
                                    )	
                                ))
                                else(select opr.region_name  from public.op_regiones opr 
                                where opr.id_region =(select oc2.id_region from public.op_comunas oc2 
                                where unaccent(lower(tcego.comuna_despacho)) = unaccent(lower(oc2.comuna_name))
                                ))
                        end as region,
                        --rsb.cant_por_producto ,
                        --rsb.bultos,
                        tcego.estado,
                        tcego.subestado
                    FROM areati.ti_carga_easy_go_opl tcego 
                    --LEFT JOIN LATERAL rutas.recupera_sku_lite(tcego.suborden) AS rsb ON true
                    WHERE tcego.fec_compromiso = current_date::date
                    AND tcego.fec_compromiso > tcego.created_at
                ) AS subquery

            )
                        
                SELECT 
                    subquery.region,
                    subquery.total_region,
                    subquery.entregados,
                    CAST(ROUND(subquery.entregados * 100.00 / subquery.total_region, 2) as float) AS ns_region
                FROM (
                    SELECT 
                        eefc.region,
                        COUNT(*) AS total_region,
                        COUNT(CASE WHEN eefc.estado = 1 THEN 1 END) AS entregados
                    FROM estatus_entregas_fec_compromiso_easy as eefc
                    GROUP BY eefc.region
                ) AS subquery;
                        
            

                         """)
            return cur.fetchall()

    def panel_no_entregados_easy(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select * from areati.panel_noentregas_easy();

                         """)
            return cur.fetchall()
        

    def panel_no_entregados_electrolux(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select * from areati.panel_noentregas_electrolux();

                         """)
            return cur.fetchall()




    def panel_principal_electrolux(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select total_entregas,total_entregados,entregados_hoy,en_ruta,
                sin_ruta_beetrack,anulados,
                CAST(porcentaje_entrega AS float),
                CAST(porcentaje_no_entrega AS float),
                CAST(proyeccion AS float)
            from areati.panel_principal_ns_electrolux();

                         """)
            return cur.fetchone()
        
    def panel_regiones_ns_electrolux(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
                        
            select region, total_region, entregados,
                CAST(ns_region AS float)
            from areati.panel_regiones_ns_electrolux();
                         """)
            return cur.fetchall()

    def codigos_obligatorios_dia(self,fecha):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
                        
            select * from rutas.codigos_obligatorios_dia('{fecha}')
                        
                         """)
            return cur.fetchall()


    def estatus_rutas_beetrack(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           select * from rutas.estatus_rutas_beetrack();                        
                         """)
            return cur.fetchall()

    def codigos_obligatorios_dia_excel(self,fecha):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
                        
            select cliente,cod_pedido,fecha_pedido,fec_reprogramada,comuna,region,descripcion,
            CASE
                WHEN sin_moradores THEN 'X'
                ELSE ''
            END AS resultado,
            CASE
                WHEN verified THEN 'X'
                ELSE ''
            END AS resultado,
            CASE
                WHEN recepcionado THEN 'X'
                ELSE ''
            END AS resultado,
            ruta_hela   
            from rutas.codigos_obligatorios_dia('{fecha}')
                        
                         """)
            return cur.fetchall()


    
    def insert_colaborador(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO transporte.colaborador
                (id_user, ids_user, tipo_razon, razon_social, rut, celular,telefono, region, comuna, direccion, representante_legal, rut_representante_legal, email_rep_legal,chofer, peoneta, giro,fecha_nacimiento_representante)
                VALUES(%(Id_user)s, %(Ids_user)s, %(Tipo_razon)s, %(Razon_social)s, %(Rut)s, %(Celular)s,
                        %(Telefono)s, %(Region)s, %(Comuna)s, %(Direccion)s, %(Representante_legal)s,
                         %(Rut_representante_legal)s, %(Email)s,false,false,%(Giro)s,%(Fecha_nacimiento)s);                
    
                 """,data)
            self.conn.commit()
    
    def activar_colab(self, rut,activar):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF Constitucion
                UPDATE transporte.colaborador
                SET activo={activar}
                WHERE rut='{rut}'
            """)
            self.conn.commit()

    def update_datos_colaborador(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
            UPDATE transporte.colaborador
            SET date_modified=CURRENT_TIMESTAMP,  celular= %(Celular)s, telefono=%(Telefono)s, region=%(Region)s, comuna= %(Comuna)s, direccion=%(Direccion)s, 
                representante_legal=%(Representante_legal)s, rut_representante_legal=%(Rut_representante_legal)s, email_rep_legal=%(Email)s, 
                activo=%(Activo)s , giro=%(Giro)s, abogado=%(Abogado)s , seguridad=%(Seguridad)s,chofer=%(Chofer)s, peoneta=%(Peoneta)s, fecha_nacimiento_representante=%(Fecha_nacimiento)s
            WHERE rut=%(Rut)s
              
                 """,data)
            self.conn.commit()

    def update_desactivar_colaborador(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
            UPDATE transporte.colaborador
            SET date_modified=CURRENT_TIMESTAMP,  activo=false,
            fecha_desvinculacion=%(Fecha_desactivacion)s, motivo_desvinculacion=%(Motivo_desactivacion)s, 
            descripcion_desvinculacion=%(Descripcion_desvinculacion)s
            WHERE rut=%(Rut)s
              
                 """,data)
            self.conn.commit()

    def obtener_estados_transporte(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
             SELECT id, estado
             FROM transporte.estado;                     
                         """)
            return cur.fetchall()
        
    

    ##Agregar PDFS a colaborador

    def agregar_pdf_colab_constitucion(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF Constitucion
                UPDATE transporte.colaborador
                SET pdf_legal_contitution='{pdf}'
                WHERE rut='{rut}'
            """)
            self.conn.commit()

    def agregar_pdf_colab_registro_comercio(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            ---  PDF Registro Comercio
                UPDATE transporte.colaborador
                SET pdf_registration_comerce='{pdf}'
                WHERE rut='{rut}'
            """)
            self.conn.commit()

    def agregar_pdf_colab_poderes(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF Vigencia Poderes
                UPDATE transporte.colaborador
                SET pdf_validity_of_powers='{pdf}'
                WHERE rut='{rut}'
            """)
            self.conn.commit()

    def agregar_pdf_colab_rrpp(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
                UPDATE transporte.colaborador
                SET pdf_certificate_rrpp='{pdf}'
                WHERE rut='{rut}'
            """)
            self.conn.commit()

    def agregar_pdf_contrato_colaborador(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE transporte.colaborador
            SET pdf_contrato='{pdf}'
            WHERE rut='{rut}'
            """)
            self.conn.commit()

    ##Agregar PDFS a Vehiculo

    def agregar_pdf_vehiculo_cert_gases(self,pdf, patente):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            ---- update cert gases
            UPDATE transporte.vehiculo
            SET pdf_gases_certification = '{pdf}'
            WHERE ppu= '{patente}'

            """)
            self.conn.commit()

    def agregar_pdf_padron(self,pdf, patente):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            ---- update padron
            UPDATE transporte.vehiculo
            SET pdf_padron = '{pdf}'
            WHERE ppu= '{patente}'

            """)
            self.conn.commit()
    
    def agregar_pdf_revision_tecnica(self,pdf, patente):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            ---- update revisión tecnica
            UPDATE transporte.vehiculo
            SET pdf_revision_tecnica  = '{pdf}'
            WHERE ppu= '{patente}'

            """)
            self.conn.commit()

    def agregar_pdf_permiso_circulacion(self,pdf, patente):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            ---- update permiso circulación
            UPDATE transporte.vehiculo
            SET registration_certificate  = '{pdf}'
            WHERE ppu= '{patente}'


            """)
            self.conn.commit()

    def agregar_pdf_soap(self,pdf, patente):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            ---- update SOAP
            UPDATE transporte.vehiculo
            SET pdf_soap  = '{pdf}'
            WHERE ppu= '{patente}'



            """)
            self.conn.commit()


    def insert_detalle_pagos(self,data):
        with self.conn.cursor() as cur:

            # que es estado ???
            cur.execute("""
                INSERT INTO transporte.detalle_pagos
                (id_user, ids_user, id_razon_social, rut_cuenta, titular_cuenta, numero_cuenta, banco, email, tipo_cuenta, forma_pago, pdf_documento, estado)
                VALUES(%(Id_user)s, %(Ids_user)s, %(Id_razon_social)s, %(Rut_titular_cta_bancaria)s, %(Titular_cta)s,
                        %(Numero_cta)s, %(Banco)s, %(Email)s, %(Tipo_cta)s,%(Forma_pago)s, '',0);                
    
                 """,data)
            self.conn.commit()

    def update_datos_detalle_pago(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
            ---- actualizar datos bancos
            UPDATE transporte.detalle_pagos
            SET rut_cuenta=%(Rut_titular_cta_bancaria)s, titular_cuenta=%(Titular_cta)s, numero_cuenta= %(Numero_cta)s, banco=%(Banco)s, email=%(Email)s,
                 tipo_cuenta=%(Tipo_cta)s, forma_pago=%(Forma_pago)s, estado=%(Estado)s
            WHERE id_razon_social=%(Id_razon_social)s
    
                 """,data)
            self.conn.commit()

    def agregar_pdf_detalle_venta(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE transporte.detalle_pagos
            SET pdf_documento='{pdf}'
            WHERE id_razon_social = (select id from transporte.colaborador where rut = '{rut}' limit 1)
            """)
            self.conn.commit()

    def insert_vehiculo_transporte(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO transporte.vehiculo
                (razon_id, ppu, marca, tipo, modelo, ano, region, comuna, disponible, activation_date, capacidad_carga_kg, capacidad_carga_m3, platform_load_capacity_kg, 
                crane_load_capacity_kg, permiso_circulacion_fec_venc, soap_fec_venc, revision_tecnica_fec_venc, validado_por_id, validado_por_ids, gps, gps_id, motivo_desvinculacion)
                VALUES(%(Razon_id)s, %(Ppu)s, %(Marca)s, %(Tipo)s, %(Modelo)s, %(Ano)s, %(Region)s, %(Comuna)s, false, %(Activation_date)s, %(Capacidad_carga_kg)s, 
                        %(Capacidad_carga_m3)s, %(Platform_load_capacity_kg)s, %(Crane_load_capacity_kg)s, %(Permiso_circulacion_fec_venc)s, %(Soap_fec_venc)s, 
                        %(Revision_tecnica_fec_venc)s,  %(Id_user)s, %(Ids_user)s,%(Gps)s,%(Id_gps)s,%(Desc_desabilitado)s);               
    
                 """,data)
            self.conn.commit()

    def update_datos_vehiculo(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
            UPDATE transporte.vehiculo
            SET update_date= CURRENT_DATE, razon_id=%(Razon_id)s, marca=%(Marca)s, tipo= %(Tipo)s,
            modelo=%(Modelo)s, ano=%(Ano)s, region=%(Region)s, comuna=%(Comuna)s, disponible=%(Disponible)s, activation_date=%(Activation_date)s, 
            capacidad_carga_kg=%(Capacidad_carga_kg)s, capacidad_carga_m3=%(Capacidad_carga_m3)s, platform_load_capacity_kg=%(Platform_load_capacity_kg)s, 
            crane_load_capacity_kg=%(Crane_load_capacity_kg)s, permiso_circulacion_fec_venc=%(Permiso_circulacion_fec_venc)s, soap_fec_venc=%(Soap_fec_venc)s, 
            revision_tecnica_fec_venc=%(Revision_tecnica_fec_venc)s,
            validado_por_id=%(Id_user)s, validado_por_ids=%(Ids_user)s, gps=%(Gps)s, gps_id=%(Id_gps)s, habilitado=%(Habilitado)s
            , motivo_desvinculacion =%(Desc_desabilitado)s
            WHERE ppu=%(Ppu)s
                 """,data)
            self.conn.commit()

    

    def update_estado_vehiculo(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
            UPDATE transporte.vehiculo
            SET disponible=%(Estado)s
            WHERE ppu=%(Ppu)s
                 """,data)
            self.conn.commit()


    def eliminar_operacion_vehiculo_asignado(self,id):
        with self.conn.cursor() as cur: 
            cur.execute(f"""
            DELETE FROM transporte.ppu_operacion
            WHERE id = {id}
                    """)
            rows_delete = cur.rowcount
        self.conn.commit() 
        
        return rows_delete
    

    def eliminar_centro_operacion(self,id):
        with self.conn.cursor() as cur: 
            cur.execute(f"""
            DELETE FROM operacion.centro_operacion
            WHERE id = {id}
                    """)
            rows_delete = cur.rowcount
        self.conn.commit() 
        
        return rows_delete
    
    def motivo_desvinculacion_colaborador(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            SELECT id, motivo
            FROM transporte.motivo_desvinculacion;
                         """)
            return cur.fetchall()
        

    def buscar_vehiculos(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           SELECT v.id, to_char(v.created_at::date, 'YYYY-MM-DD')  , v.update_date, v.razon_id, v.ppu, coalesce (v.marca, 0), coalesce (v.tipo, 0 ), v.modelo, 
                v.ano, coalesce (v.region, 0) , coalesce (v.comuna, 0), v.disponible, v.activation_date,
                COALESCE(v.capacidad_carga_kg, 0),COALESCE(v.capacidad_carga_m3, 0),
                COALESCE(v.platform_load_capacity_kg, 0), COALESCE(v.crane_load_capacity_kg, 0) , 
                v.permiso_circulacion_fec_venc, v.soap_fec_venc, 
                v.revision_tecnica_fec_venc, v.registration_certificate, v.pdf_revision_tecnica, 
                v.pdf_soap, v.pdf_padron, v.pdf_gases_certification, v.validado_por_id, 
                v.validado_por_ids , c.razon_social, c.rut , v.gps, g.id , g.imei, g.fec_instalacion , g.oc_instalacion , v.habilitado, v.disponible, v.motivo_desvinculacion,
                g.fec_baja, g.oc_baja, coalesce(re.region_name,'S/I'), coalesce (tv.tipo, 'S/I' )
            FROM transporte.vehiculo v
            left join transporte.colaborador c on v.razon_id = c.id    
            left join transporte.gps g on v.gps_id = g.id   
            left join transporte.tipo_vehiculo tv on v.tipo  = tv.id  
            left join public.op_regiones re on cast(v.region as varchar) = re.id_region           
                         """)
            return cur.fetchall()
        
    def asignar_operacion_a_vehiculo(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
                        
            INSERT INTO transporte.ppu_operacion
            ( id_user, ids_user, id_ppu, id_operacion, id_centro_op, estado)
            VALUES(%(Id_user)s, %(Ids_user)s,%(Id_ppu)s, %(Id_operacion)s,%(Id_centro)s,%(Estado)s);

            --UPDATE transporte.vehiculo
           -- SET agency_id=
           -- WHERE id=
                 """,data)
            self.conn.commit()

    def cambiar_estado_a_vehiculo(self,id):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                        
            UPDATE transporte.vehiculo
            SET disponible = NOT disponible
            WHERE id= {id}
                 """)
            self.conn.commit()

    def cambiar_estado_a_usuario_tripulacion(self,id):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                        
            UPDATE transporte.usuarios 
            SET activo = CASE 
                            WHEN activo IS NULL THEN TRUE
                            ELSE NOT activo 
                        END
            WHERE id= {id}
                 """)
            self.conn.commit()


    def revisar_operacion_asignada_a_vehiculo(self,id_ppu):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           SELECT id, created_at, id_user, ids_user, id_ppu, id_operacion, id_centro_op, estado
            FROM transporte.ppu_operacion
            WHERE id_ppu={id_ppu};      
                         """)
            return cur.fetchall()

        

    def buscar_id_colab_por_rut(self,rut):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           SELECT id, razon_social  from transporte.colaborador where rut = '{rut}'         
                         """)
            return cur.fetchone()
    
    ### esto es para mostrar los datos de los colab en formularios y rellenar
    def buscar_colab_registrados(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           select id, razon_social, region ,comuna ,telefono  from transporte.colaborador        
                         """)
            return cur.fetchone()
        
    ### Verificar que el colaborador no se encuentre registrado en una tabla 

    def verificar_colab_registrado_tripulacion(self,rut):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           SELECT *  from transporte.usuarios u  where rut = '{rut}' 
       
                         """)
            return cur.fetchone()
    
    def verificar_colab_registrado_vehiculos(self,rut):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           SELECT *  from transporte.vehiculos u  where rut = '{rut}' 
       
                         """)
            return cur.fetchone()

        
    def buscar_colaboradores_por_nombre(self,nombre):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           SELECT col.id, to_char(col.created_at,'dd/mm/yyyy'), col.id_user, col.ids_user, to_char(col.date_modified,'dd/mm/yyyy'), r.nombre as "tipo razon" , col.razon_social, col.rut, col.celular, col.telefono, col.region, col.comuna, col.direccion, representante_legal, rut_representante_legal, email_rep_legal, direccion_comercial, pdf_legal_contitution, pdf_registration_comerce, pdf_validity_of_powers, pdf_certificate_rrpp, chofer, peoneta, abogado, seguridad, activo, giro, 
                pdf_contrato,
                (SELECT count(*) FROM transporte.vehiculo v WHERE v.razon_id = col.id) AS vehiculos,
                (SELECT count(*) FROM transporte.usuarios u WHERE u.id_razon_social = col.id) AS tripulacion,
                (SELECT array_agg(json_build_object('patente', v.ppu, 'tipo', v.tipo, 'gps', v.gps, 'disponible', v.disponible, 'habilitado', v.habilitado))
                    FROM transporte.vehiculo v
                    WHERE v.razon_id = col.id) AS patentes,
                (SELECT array_agg(json_build_object('nombre', u.nombre_completo, 'tipo', u.tipo_usuario, 'activo', u.activo ))
                    FROM transporte.usuarios u
                    WHERE u.id_razon_social = col.id) AS usuarios,
                fecha_nacimiento_representante  
            FROM transporte.colaborador col
            left join hela.rol r ON col.tipo_razon  = r.id 
            left join transporte.vehiculo v ON v.razon_id = col.id
            where col.rut = '{nombre}' or lower(col.razon_social) like lower('%{nombre}%') 
            group by col.id, col.razon_social, col.tipo_razon, col.rut, col.created_at, col.activo, r.nombre
            ORDER by col.id;       
                         """)
            return cur.fetchall()

    def buscar_vehiculos_por_filtro(self,filtro):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           
            SELECT v.id, to_char(v.created_at::date, 'YYYY-MM-DD')  , v.update_date, v.razon_id, v.ppu, coalesce (v.marca, 0), coalesce (v.tipo, 0 ), v.modelo, 
                v.ano, coalesce (v.region, 0) , coalesce (v.comuna, 0), v.disponible, v.activation_date,
                COALESCE(v.capacidad_carga_kg, 0),COALESCE(v.capacidad_carga_m3, 0),
                COALESCE(v.platform_load_capacity_kg, 0), COALESCE(v.crane_load_capacity_kg, 0) , 
                v.permiso_circulacion_fec_venc, v.soap_fec_venc, 
                v.revision_tecnica_fec_venc, v.registration_certificate, v.pdf_revision_tecnica, 
                v.pdf_soap, v.pdf_padron, v.pdf_gases_certification, v.validado_por_id, 
                v.validado_por_ids , c.razon_social, c.rut , v.gps, g.id , g.imei, g.fec_instalacion , g.oc_instalacion , v.habilitado, v.disponible, v.motivo_desvinculacion,
                g.fec_baja, g.oc_baja, coalesce(re.region_name,'S/I'), coalesce (tv.tipo, 'S/I' )
            FROM transporte.vehiculo v
            left join transporte.colaborador c on v.razon_id = c.id    
            left join transporte.gps g on v.gps_id = g.id   
            left join transporte.tipo_vehiculo tv on v.tipo  = tv.id  
            left join public.op_regiones re on cast(v.region as varchar) = re.id_region  
            where lower(v.ppu) like lower('%{filtro}%') or lower(c.razon_social) like lower('%{filtro}%') or  c.rut = '{filtro}'     
                         """)
            return cur.fetchall()
        
    def buscar_vehiculos_y_operacion_pta(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
             SELECT v.id, to_char(v.created_at::date, 'YYYY-MM-DD')  , v.update_date, v.razon_id, v.ppu, coalesce (v.marca, 0),coalesce (v.tipo, 0 ), v.modelo, 
                v.ano, coalesce (v.region, 0),  coalesce (v.comuna, 0), v.disponible, v.activation_date, v.capacidad_carga_kg, v.capacidad_carga_m3, 
                v.platform_load_capacity_kg, v.crane_load_capacity_kg, v.permiso_circulacion_fec_venc, v.soap_fec_venc, 
                v.revision_tecnica_fec_venc, v.registration_certificate, v.pdf_revision_tecnica, 
                v.pdf_soap, v.pdf_padron, v.pdf_gases_certification, v.validado_por_id, 
                v.validado_por_ids , c.razon_social, c.rut , v.gps, g.id , g.imei, g.fec_instalacion , g.oc_instalacion , v.habilitado, v.disponible, 
                array_agg(po.id_operacion), array_agg(po.id_centro_op)
                FROM transporte.vehiculo v
            left join transporte.colaborador c on v.razon_id = c.id    
            left join transporte.gps g on v.gps_id = g.id   
            left join transporte.ppu_operacion po  on v.id = po.id_ppu
            group by v.id, c.razon_social,c.rut,
            g.id , g.imei, g.fec_instalacion , g.oc_instalacion
            order by 1 
                                  
                         """)
            return cur.fetchall()
        
    def buscar_colaboradores(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            SELECT col.id, to_char(col.created_at,'dd/mm/yyyy'), col.id_user, col.ids_user, to_char(col.date_modified,'dd/mm/yyyy'), r.nombre as "tipo razon" , col.razon_social, col.rut, col.celular, col.telefono, col.region, col.comuna, col.direccion, representante_legal, rut_representante_legal, email_rep_legal, direccion_comercial, pdf_legal_contitution, pdf_registration_comerce, pdf_validity_of_powers, pdf_certificate_rrpp, chofer, peoneta, abogado, seguridad, activo, giro, 
                pdf_contrato,
                (SELECT count(*) FROM transporte.vehiculo v WHERE v.razon_id = col.id) AS vehiculos,
                (SELECT count(*) FROM transporte.usuarios u WHERE u.id_razon_social = col.id) AS tripulacion,
                (SELECT array_agg(json_build_object('patente', v.ppu, 'tipo', v.tipo, 'gps', v.gps, 'disponible', v.disponible, 'habilitado', v.habilitado))
                    FROM transporte.vehiculo v
                    WHERE v.razon_id = col.id) AS patentes,
                (SELECT array_agg(json_build_object('nombre', u.nombre_completo, 'tipo', u.tipo_usuario, 'activo', u.activo ))
                    FROM transporte.usuarios u
                    WHERE u.id_razon_social = col.id) AS usuarios,
                fecha_nacimiento_representante    
            FROM transporte.colaborador col
            left join hela.rol r ON col.tipo_razon  = r.id 
            left join transporte.vehiculo v ON v.razon_id = col.id
            group by col.id, col.razon_social, col.tipo_razon, col.rut, col.created_at, col.activo, r.nombre
            ORDER by col.id;
                                  
                         """)
            return cur.fetchall()
        
    def buscar_detalle_pago(self, id):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            SELECT id, to_char(created_at::date, 'YYYY-MM-DD'), id_user, ids_user, id_razon_social, rut_cuenta, titular_cuenta, numero_cuenta, banco, email, tipo_cuenta, forma_pago, pdf_documento, estado
            FROM transporte.detalle_pagos
            WHERE id_razon_social = {id} or rut_cuenta = '{id}';

                                  
                         """)
            return cur.fetchall()

    def insert_bitacora_transporte(self,data):
        with self.conn.cursor() as cur:

            cur.execute("""
                INSERT INTO transporte.bitacora_general
                (id_usuario, ids_usuario, modificacion, latitud, longitud, origen)
                VALUES(%(Id_user)s, %(Ids_user)s, %(Modificacion)s, %(Latitud)s, %(Longitud)s, 
                       %(Origen)s);
 
                 """,data)
            self.conn.commit()


    ### conexion modalidades-operaciones (lo dejo aca porque no me dejo guardar como router)

    def agregar_razon_social(self,data):
        with self.conn.cursor() as cur:

            cur.execute("""
                INSERT INTO operacion.modalidad_operacion(id_user, ids_user, nombre, description, creation_date, update_date, estado ) 
                VALUES (%(id_user)s, %(ids_user)s,%(nombre)s, %(description)s,%(creation_date)s,%(update_date)s,%(estado)s)

                 """,data)
            self.conn.commit()

    def buscar_modalidad_operacion(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            --select * from operacion.modalidad_operacion mo
            SELECT mo.id, mo.created_at, mo.id_user, mo.ids_user, mo.nombre,
                    mo.description, mo.creation_date, mo.update_date, mo.estado, count(co.id) as cant_co, color_hex
            FROM operacion.modalidad_operacion mo
            left join operacion.centro_operacion co on co.id_op = mo.id 
            group by mo.id
            order by cant_co desc,mo.nombre,mo.estado
                                  
                         """)
            return cur.fetchall()

    def filtrar_modalidad_operacion_por_nombre(self, nombre):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select * from operacion.modalidad_operacion mo  
            where nombre like '%{nombre}%' 
                                  
                         """)
            return cur.fetchall()
        
    def delete_modalidad_operacion(self, id): 
        with self.conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM operacion.modalidad_operacion WHERE id = {id};
                        """)
            rows_delete = cur.rowcount
        self.conn.commit() 
        return rows_delete
        

    def actualizar_modalidad_operacion(self, estado, id): 
        with self.conn.cursor() as cur:
            cur.execute(f"""
                UPDATE operacion.modalidad_operacion SET estado={estado} WHERE id={id}
                        """)
            rows_delete = cur.rowcount
        self.conn.commit() 
        return rows_delete
    

    ### ingresar usuarios de transporte (tripulacion)
    

    def agregar_usuario_transporte(self,data):
        with self.conn.cursor() as cur:

            cur.execute("""
                INSERT INTO transporte.usuarios
                ( id_ingreso_hela, id_user, ids_user, id_razon_social, nombre_completo, rut, nroseriecedula, email, telefono, birthday, region, comuna, domicilio, 
                tipo_usuario, fec_venc_lic_conducir, activo, validacion_seguridad, validacion_transporte)
                VALUES( %(Id_ingreso_hela)s, %(Id_user)s, %(Ids_user)s,%(Id_razon_social)s, %(Nombre_completo)s, %(Rut)s,%(Nro_serie_cedula)s,  %(Email)s,%(Telefono)s,%(Birthday)s, 
                        %(Region)s, %(Comuna)s,%(Domicilio)s, %(Tipo_usuario)s,%(Fec_venc_lic_conducir)s, %(Activo)s, %(Validacion_seguridad)s, %(Validacion_transporte)s);

                 """,data)
            self.conn.commit()
    
    def update_datos_usuario(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""

                        
            UPDATE transporte.usuarios
            SET nombre_completo=%(Nombre_completo)s, nroseriecedula=%(Nro_serie_cedula)s, email=%(Email)s, telefono= %(Telefono)s, birthday=%(Birthday)s, 
            region=%(Region)s, comuna= %(Comuna)s, domicilio=%(Domicilio)s, tipo_usuario=%(Tipo_usuario)s, fec_venc_lic_conducir=%(Fec_venc_lic_conducir)s, 
            validacion_seguridad=%(Validacion_seguridad)s, validacion_transporte= %(Validacion_transporte)s
            
            WHERE  rut=%(Rut)s



                 """,data)
            self.conn.commit()

    def lista_usuario_transporte(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            
            SELECT u.id, to_char(u.created_at::date, 'YYYY-MM-DD') AS created_at, u.id_ingreso_hela, u.id_user, u.ids_user, u.id_razon_social, u.jpg_foto_perfil, u.nombre_completo, u.rut, u.nroseriecedula, 
                u.email, u.telefono, u.birthday, u.region, u.comuna, u.domicilio, u.tipo_usuario, u.pdf_antecedentes, u.pdf_licencia_conducir, u.fec_venc_lic_conducir, u.pdf_cedula_identidad, u.pdf_contrato,
                u.activo, u.validacion_seguridad, u.validacion_transporte, coalesce (c.razon_social, '') as razon_social, c.rut 
            FROM transporte.usuarios u
            left join transporte.colaborador c on u.id_razon_social = c.id
                                  
                         """)
            return cur.fetchall()
        
    

    ### agregar ruta de documentos


    def agregar_jpg_foto_perfil(self,jpg, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE transporte.usuarios
            SET jpg_foto_perfil='{jpg}'
            WHERE rut = '{rut}' 
            """)
            self.conn.commit()

    def agregar_pdf_contrato(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE transporte.usuarios
            SET pdf_contrato='{pdf}'
            WHERE rut = '{rut}' 
            """)
            self.conn.commit()

    def agregar_pdf_cedula_identidad(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE transporte.usuarios
            SET pdf_cedula_identidad='{pdf}'
            WHERE rut = '{rut}' 
            """)
            self.conn.commit()

    def agregar_pdf_licencia_conducir(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE transporte.usuarios
            SET pdf_licencia_conducir='{pdf}'
            WHERE rut = '{rut}' 
            """)
            self.conn.commit()
    
    def agregar_pdf_antecedentes(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE transporte.usuarios
            SET pdf_antecedentes='{pdf}'
            WHERE rut = '{rut}' 
            """)
            self.conn.commit()

    #### agregar Centro de operacion
    def agregar_centro_operacion(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" 
            INSERT INTO operacion.centro_operacion
            (id_user, ids_user, centro, descripcion, region, id_op)
            VALUES(%(Id_user)s, %(Ids_user)s, %(Centro)s, %(Descripcion)s,%(Region)s, %(Id_op)s);

            """, data)
            self.conn.commit()

    def mostrar_todos_centros_operacion(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            
                SELECT id, to_char(created_at::date, 'YYYY-MM-DD'), id_user, ids_user, id_op, centro, descripcion, r.region_name 
                FROM operacion.centro_operacion co
                left join public.op_regiones r on co.region::VARCHAR = r.id_region                
                         """)
            return cur.fetchall()


    def mostrar_centros_operacion(self,id_op):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            
                SELECT id, to_char(created_at::date, 'YYYY-MM-DD'), id_user, ids_user, id_op, centro, descripcion, r.region_name 
                FROM operacion.centro_operacion co
                left join public.op_regiones r on co.region::VARCHAR = r.id_region 
                where id_op = {id_op}

                                  
                         """)
            return cur.fetchall()
        
    def mostrar_centros_operacion_asignado_a_vehiculo(self,id_op,id_vehiculo):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
                        
                with estado_ppu AS
                (SELECT ppu.id, ppu.estado,ppu.id_operacion, ppu.id_ppu, ppu.id_centro_op 
                FROM transporte.ppu_operacion ppu 
                WHERE ppu.id_ppu =  {id_vehiculo} )

                SELECT co.id, co.created_at, co.id_user, co.ids_user, co.id_op, co.centro, 
                    co.descripcion, r.region_name, ep.estado, ep.id
                FROM operacion.centro_operacion co
                LEFT JOIN public.op_regiones r ON co.region::VARCHAR = r.id_region 
                left join estado_ppu ep on co.id_op = ep.id_operacion and co.id = ep.id_centro_op
            
               -- SELECT co.id, co.created_at, co.id_user, co.ids_user, co.id_op, co.centro, 
                 --   co.descripcion, r.region_name,
                 --   (SELECT estado FROM transporte.ppu_operacion ppu WHERE ppu.id_operacion = co.id_op AND ppu.id_ppu = {id_vehiculo} and ppu.id_centro_op = co.id LIMIT 1) AS estado
               -- FROM operacion.centro_operacion co
               -- LEFT JOIN public.op_regiones r ON co.region::VARCHAR = r.id_region 
               --- WHERE co.id_op = {id_op};
            
                         """)
            return cur.fetchall()


    ####Agregar datos GPS Vehiculo

    def agregar_datos_gps(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" 
            INSERT INTO transporte.gps
            (imei, fec_instalacion, oc_instalacion, id_user, ids_user, fec_baja,oc_baja)
            VALUES( %(Imei)s, %(Fecha_instalacion)s,%(Oc_instalacion)s,%(Id_user)s, %(Ids_user)s, %(Fecha_desinstalacion)s, %(Oc_desinstalacion)s);

            """, data)
            self.conn.commit()

    ### actualiza datos si se actualizan los datos del gps
    def actualizar_datos_gps(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" 
                        
            UPDATE transporte.gps
            SET  imei= %(Imei)s, fec_instalacion=%(Fecha_instalacion)s, oc_instalacion=%(Oc_instalacion)s, 
                id_user=%(Id_user)s, ids_user=%(Ids_user)s ,fec_baja=%(Fecha_desinstalacion)s ,oc_baja=%(Oc_desinstalacion)s
            WHERE id=%(Id_gps)s

            """, data)
            self.conn.commit()

 ### actualiza datos si se desactivan los datos del gps
    def actualizar_datos_gps_si_se_desactiva_gps(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" 
                        
            UPDATE transporte.gps
            SET imei= %(Imei)s, fec_baja=%(Fecha_desinstalacion)s, oc_baja=%(Oc_desinstalacion)s
            WHERE id=%(Id_gps)s

            """, data)
            self.conn.commit()

    ### eliminar gps creado  en caso de que no se agregue correctamente la patente 
    def eliminar_gps(self,imei):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                        
            DELETE FROM transporte.gps
            WHERE imei= '{imei}'

            """)
            self.conn.commit()

    def get_max_id_gps(self) :
        with self.conn.cursor() as cur:
            cur.execute("""
              select coalesce (max(id),1) from transporte.gps g
            """)

            return cur.fetchone()
        
    #### buscar ppu operacion para buscador

    def buscar_vehiculos_ppu_operacion(self,id_operacion, id_co):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           select id_ppu from transporte.ppu_operacion po 
            where po.id_operacion = {id_operacion} or po.id_centro_op = {id_co} 
       
                         """)
            return cur.fetchall()
        
    def buscar_vehiculos_ppu_operacion_co(self,id_operacion, id_co):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           select id_ppu from transporte.ppu_operacion po 
            where po.id_operacion = {id_operacion} and po.id_centro_op = {id_co} 
       
                         """)
            return cur.fetchall()
    ### Peso Volumetrico

    def insert_peso_volumetrico_sku(self, body):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            INSERT INTO operacion.pv_sku (sku, descripcion, alto, ancho, profundidad, peso_kg, bultos, id_user, ids_user) 
            VALUES ('{body.sku}','{body.descripcion}',{body.alto},{body.ancho},{body.profundidad},{body.peso_kg},{body.bultos},{body.id_user},'{body.ids_user}')          """)
        self.conn.commit() 

    def buscar_entrada_sku(self,sku_descripcion):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           select * from operacion.busca_sku_entrada('{sku_descripcion}'); 
       
                         """)
            return cur.fetchall()
        
    def buscar_sku_o_descripcion(self,sku):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           select * from operacion.buscar_sku_o_descripcion('{sku}')
       
                         """)
            return cur.fetchall()
        

    ####

    def buscar_centro_operacion_usuario(self,id_usuario):
        with self.conn.cursor() as cur:
            cur.execute(f"""   

            SELECT 
                mo.nombre as "Operacion", 
                jsonb_agg(
                    jsonb_build_object(
                    'nombre', co.centro,
                    'id', co.id
                    )
                ) AS centros
            FROM 
                operacion.centro_operacion co
            LEFT JOIN 
                operacion.modalidad_operacion mo 
                ON co.id_op = mo.id
                 WHERE '{id_usuario}' = ANY (co.ids_coordinador)
            GROUP BY 
                mo.nombre;
          
                         """)
            return cur.fetchall()

    def buscar_centro_operacion_lista(self,id_usuario):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
        ------CENTRO OPERACIONES EXCLUYENDO A LA ID ENTREGADA
            SELECT co.id, co.id_op, co.centro, 
                co.descripcion, r.region_name
            FROM operacion.centro_operacion co
            LEFT JOIN public.op_regiones r ON co.region::VARCHAR = r.id_region 
            where  ({id_usuario} <> ALL(co.id_coordinador) or co.id_coordinador is null);

          
       
                         """)
            return cur.fetchall()
        

    def buscar_datos_supervisores_hela(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
        SELECT u.id, u.nombre ,mail, imagen_perfil ,
            operacion.obtener_centros_por_portal('hela-' || u.id) AS centros
        FROM hela.usuarios u
        where rol_id in ('80','81','91') and activate = true
                         """)
            return cur.fetchall()

    def asignar_coordinador_centro_operacion(self, data): 
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE operacion.centro_operacion as co
                SET id_coordinador = array_append(COALESCE(id_coordinador, ARRAY[]::bigint[]), %(Id_usuario)s::bigint),
                    ids_coordinador = array_append(COALESCE(ids_coordinador, ARRAY[]::varchar[]), %(Ids_usuario)s)
                WHERE id = %(Id_co)s;
                        """,data)
            rows_delete = cur.rowcount
        self.conn.commit() 
        return rows_delete

    def eliminar_coordinador_centro_operacion(self, data): 
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE operacion.centro_operacion as co
                SET id_coordinador = array_remove(COALESCE(id_coordinador, ARRAY[]::bigint[]), %(Id_usuario)s::bigint),
                    ids_coordinador = array_remove(COALESCE(ids_coordinador, ARRAY[]::varchar[]), %(Ids_usuario)s)
                WHERE id = %(Id_co)s;
                        """,data)
            rows_delete = cur.rowcount
        self.conn.commit() 
        return rows_delete
    

    ##### MELI

    def datos_excel_meli_base(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           SELECT * from mercadolibre.datos_excel_base deb                        
                         """)
            return cur.fetchall()
        
    def lista_conductores(self, id_ppu):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           SELECT * FROM mercadolibre.obtener_usuarios_con_telefono_formateado({id_ppu});                     
                         """)
            return cur.fetchall()
        
    def lista_peonetas(self,fecha):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
          select u.id, u.nombre_completo from transporte.usuarios u 
          where u.tipo_usuario = 2 AND u.id NOT in 
          (select id_peoneta FROM mercadolibre.citacion c WHERE fecha = '{fecha}'::date and c.id_peoneta notnull);                  
                         """)
            return cur.fetchall()
        
    def citacion_operacion_fecha(self,fecha: str, id : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
          select * from mercadolibre.citacion_operacion_fecha('{fecha}', {id});                 
                         """)
            return cur.fetchall()
        
    def lista_estado_citaciones(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
          select * from mercadolibre.estados_citacion ec                  
                         """)
            return cur.fetchall()
        
    def lista_estado_citaciones(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
          select * from mercadolibre.estados_citacion ec                  
                         """)
            return cur.fetchall()
        
    
    def recupera_citacion_cop(self,fecha: str, op : int, cop : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
          select * from mercadolibre.recupera_citacion_cop('{fecha}',{op},{cop})              
                         """)
            return cur.fetchall()
    
    def estado_citaciones_por_id(self,id_estado):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
         select estado from mercadolibre.estados_citacion ec where id = {id_estado};                 
                         """)
            return cur.fetchall()
        
    def insert_patente_citacion(self, body):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            INSERT INTO mercadolibre.citacion (id_user, ids_user, fecha, id_ppu, id_operacion, id_centro_op, tipo_ruta, estado) VALUES ({body.id_user},'{body.ids_user}','{body.fecha}',{body.id_ppu},{body.id_operacion},{body.id_centro_op},{body.tipo_ruta},{body.estado})
                        """)
        self.conn.commit() 

    def update_estado_patente_citacion(self, estado: int, id : int, fecha:str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
         UPDATE mercadolibre.citacion SET estado={estado} WHERE fecha='{fecha}' AND id='{id}'         
                        """)
        self.conn.commit() 

    def borrar_patente_citacion(self, id: str,fecha):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            delete from mercadolibre.citacion where id = {id} and fecha = '{fecha}'
                        """)
        self.conn.commit() 

    def update_estado_ruta_meli_citacion(self, ruta_meli: int, id : int, fecha: str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            UPDATE mercadolibre.citacion SET ruta_meli ={ruta_meli} WHERE id={id} and fecha='{fecha}'
                        """)
        self.conn.commit() 

    
    def recuperar_patentes_citacion(self,op : int, cop : int, fecha: str):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
         select * from mercadolibre.recuperar_patentes_citacion({op},{cop},'{fecha}');              
                         """)
            return cur.fetchall()


    def filtrar_centro_op_por_id_op(self,op : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
         select id, centro from  operacion.centro_operacion co where id_op = {op};              
                         """)
            return cur.fetchall()
        

    def filtrar_citacion_por_op_y_cop(self,id_operacion: str, id_centro_op : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
         select * from mercadolibre.citacion c  where id_operacion = {id_operacion} and id_centro_op = {id_centro_op}
              
                         """)
            return cur.fetchall()
        
        
    def obtener_tipo_ruta_meli(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
                   select * from mercadolibre.tipo_ruta tr  
                         """)
            return cur.fetchall()
        
    

    def update_ingresar_driver_peoneta(self, id_driver: int, id_ppu: int, fecha: str, id_peoneta):
        with self.conn.cursor() as cur:

            if id_peoneta is not None:
                cur.execute(f"""
                UPDATE mercadolibre.citacion SET id_driver = {id_driver}, id_peoneta = {id_peoneta}  WHERE fecha='{fecha}' AND id_ppu={id_ppu}
                        """)
            else:
                cur.execute(f"""
                UPDATE mercadolibre.citacion SET id_driver = {id_driver} WHERE fecha='{fecha}' AND id_ppu={id_ppu}
                        """)
                
        self.conn.commit()
        
    def update_tipo_ruta_citacion(self, tipo_ruta: int, id : int, fecha: str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            UPDATE mercadolibre.citacion SET tipo_ruta ={tipo_ruta} WHERE id={id} and fecha='{fecha}'
                      """)
            rows_delete = cur.rowcount
        self.conn.commit()
        return rows_delete

    def obtener_veh_disp_operaciones(self,fecha):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                          
           SELECT * FROM transporte.obtener_veh_disp_operaciones(ARRAY[38, 39], '{fecha}')
       
                         """)
            return cur.fetchall() 
        

    def contar_citaciones_co_por_fecha(self,fecha:str, id_cop:int):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                          
            SELECT COUNT(*) FROM mercadolibre.citacion c WHERE c.fecha = '{fecha}'  AND c.id_centro_op ={id_cop} 
       
                         """)
            return cur.fetchall()    
        
    def contar_citaciones_co_confirmadas_por_fecha(self,fecha:str, id_cop:int, estado: int):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                          
            SELECT COUNT(*) FROM mercadolibre.citacion c WHERE c.fecha = '{fecha}'  AND c.id_centro_op ={id_cop} AND c.estado = {estado}
       
                         """)
            return cur.fetchall()   


    def update_citacion_ambulancia(self,id_ppu_amb: int, ruta_meli_amb:int, ruta_amb_interna:int, id_ppu : int, fecha: str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            UPDATE mercadolibre.citacion SET id_ppu_amb = {id_ppu_amb},ruta_meli_amb =  {ruta_meli_amb}, ruta_amb_interna = {ruta_amb_interna} WHERE id_ppu={id_ppu} and fecha='{fecha}'
                      """)
        self.conn.commit()

    def update_citacion_ruta_meli_amb(self,ruta_amb_interna: str, id_ppu: int, fecha: str, id_ppu_amb: int, ruta_meli_amb:str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           UPDATE mercadolibre.citacion SET ruta_amb_interna = '{ruta_amb_interna}', id_ppu_amb = {id_ppu_amb}, ruta_meli_amb = '{ruta_meli_amb}' where id_ppu = {id_ppu} and fecha ='{fecha}'                    
           """)
        self.conn.commit()


    def insert_datos_excel_prefactura_meli_diario_lh(self, id_usuario : int,ids_usuario : str,fecha :int,latitud : str,longitud : str,body):

        with self.conn.cursor() as cur:
            query = """
            INSERT INTO mercadolibre.ingreso_diario_textual_lh
            (id_usuario, ids_usuario, fecha_ingreso, list_routes_steps__route_id_title, list_routes_steps__travel_id, list_routes_steps__rostering_id, list_routes_steps__rostering_id_2, list_routes_steps__vehicles_plates_title,
             list_routes_steps__vehicles_plates_title_2, andes_visually_hidden_2, list_routes_steps__vehicles_plates_title_3, andes_tooltip__trigger_src, gps_status__title, andes_visually_hidden_4, list_routes_steps__location_title, andes_tooltip__trigger_2,
             label_with_tooltip__title, label_with_tooltip__area_real_time, label_with_tooltip__tooltip, label_with_tooltip__tooltip_2, list_routes_steps__location_title_2, andes_tooltip__trigger_3, andes_tooltip__trigger_4, label_with_tooltip__title_3, label_with_tooltip__area_real_time_2,
             label_with_tooltip__tooltip_3, label_with_tooltip__tooltip_4, list_routes_steps__sub_status_title, andes_tooltip__trigger_5, andes_tooltip__trigger_6, label_with_tooltip__title_4, label_with_tooltip__title_5, label_with_tooltip__title_6, andes_tooltip__trigger_7, andes_tooltip__trigger_8, 
             label_with_tooltip__title_7, latitud, longitud)

            VALUES %s
            """
            values = [
            (id_usuario, ids_usuario, fecha, 
            item.get('list-routes-steps__route-id-title', None),
            item.get('list-routes-steps__travel-id', None),
            item.get('list-routes-steps__rostering-id', None),
            item.get('list-routes-steps__rostering-id 2', None),
            item.get('list-routes-steps__vehicles-plates-title', None),
            item.get('list-routes-steps__vehicles-plates-title 2', None),
            item.get('andes-visually-hidden 2', None),
            item.get('list-routes-steps__vehicles-plates-title 3', None),
            item.get('andes-tooltip__trigger src', None),
            item.get('gps-status__title', None),
            item.get('andes-visually-hidden 4', None),
            item.get('list-routes-steps__location-title', None),
            item.get('andes-tooltip__trigger 2', None),
            item.get('label-with-tooltip__title', None),
            item.get('label-with-tooltip__area-real-time', None),
            item.get('label-with-tooltip__tooltip', None),
            item.get('label-with-tooltip__tooltip 2', None),
            item.get('list-routes-steps__location-title 2', None),
            item.get('andes-tooltip__trigger 3', None),
            item.get('andes-tooltip__trigger 4', None),
            item.get('label-with-tooltip__title 3', None),
            item.get('label-with-tooltip__area-real-time 2', None),
            item.get('label-with-tooltip__tooltip 3', None),
            item.get('label-with-tooltip__tooltip 4', None),
            item.get('list-routes-steps__sub-status-title', None),
            item.get('andes-tooltip__trigger 5', None),
            item.get('andes-tooltip__trigger 6', None),
            item.get('label-with-tooltip__title 4', None),
            item.get('label-with-tooltip__title 5', None),
            item.get('label-with-tooltip__title 6', None),
            item.get('andes-tooltip__trigger 7', None),
            item.get('andes-tooltip__trigger 8', None),
            item.get('label-with-tooltip__title 7', None),
            latitud, longitud
            )
              
            for item in body
                    ]
            execute_values(cur, query, values)

        self.conn.commit()
        

    def insert_datos_excel_prefactura_meli_diario_fm(self, id_usuario : int,ids_usuario : str,fecha :int,latitud : str,longitud : str,body):

        with self.conn.cursor() as cur:
            query = """
            INSERT INTO mercadolibre.ingreso_diario_textual_fm
            (id_usuario, ids_usuario, fecha_ingreso, monitoring_row_higher_details__text, monitoring_row_higher_details__text_2, button_copy__text, sc_progress_wheel__percentage, 
             monitoring_row_higher_details__text_3, monitoring_row_higher_details__text_4, monitoring_row_higher_details__text_7, bold, gray, monitoring_row_higher_details__text_pipe_2, gray_2, monitoring_row_higher_details, monitoring_row_higher_details__text_9, monitoring_row_lower_details, andes_tooltip__trigger, andes_visually_hidden, monitoring_row_higher_details__text_pipe_3, andes_tooltip__trigger_2, performance_tooltip, performance_tooltip_2, monitoring_row_lower_details_2,
             third_item, monitoring_row_lower_details_4, monitoring_row_higher_details__text_10,latitud,longitud)
            VALUES %s
            """
            values = [
            (id_usuario, ids_usuario, fecha,item.get('monitoring-row-higher-details__text', None),item.get('monitoring-row-higher-details__text 2', None),item.get('button-copy__text', None),item.get('sc-progress-wheel__percentage', None),
            item.get('monitoring-row-higher-details__text 3', None),item.get('monitoring-row-higher-details__text 4', None),item.get('monitoring-row-higher-details__text 7', None), item.get('bold', None),item.get('gray', None),
            item.get('monitoring-row-higher-details__text-pipe 2', None),item.get('gray 2', None),item.get('monitoring-row-higher-details', None),item.get('monitoring-row-higher-details__text 9', None),item.get('monitoring-row-lower-details', None),item.get('andes-tooltip__trigger', None),
            item.get('andes-visually-hidden', None),item.get('monitoring-row-higher-details__text-pipe 3', None),item.get('andes-tooltip__trigger 2', None),item.get('performance-tooltip', None),
            item.get('performance-tooltip 2', None),item.get('monitoring-row-lower-details 2', None),item.get('third-item', None),item.get('monitoring-row-lower-details 4', None),
            item.get('monitoring-row-higher-details__text 10', None),latitud, longitud
                )                
            for item in body
                    ]
            execute_values(cur, query, values)

        self.conn.commit()


    def insert_datos_excel_prefactura_meli_diario_lm(self, id_usuario : int,ids_usuario : str,fecha :int,latitud : str,longitud : str,body):

        def verify_int(value, default=0):
            try:
                # Intentamos convertir el valor a int. Si tiene éxito, devolvemos el valor original.
                int(value)
                return value
            except (ValueError, TypeError):
                # Si no es posible, devolvemos el valor por defecto (0 o None).
                return default
        
        with self.conn.cursor() as cur:
            query = """
            INSERT INTO mercadolibre.ingreso_diario_textual_lm
            (id_usuario, ids_usuario, fecha_ingreso, monitoring_row__bold, monitoring_row_details__driver_name, sc_progress_wheel__percentage, monitoring_row_shipments__delivered_packages_text_2, monitoring_row_shipments__delivered_packages_text_3, monitoring_row_shipments__delivered_packages_text_4, monitoring_row_shipments__packages_2, andes_visually_hidden_2, metric_box__value_principal, metric_box__value_principal_2, metric_box__value_principal_3, metric_box__value_principal_4, metric_box__value_principal_5, metric_box__value_principal_6, andes_visually_hidden_4, metric_box__value_principal_7, monitoring_row_details__name, monitoring_row_details__untracked, monitoring_row__chevron_open_src, monitoring_row_details__license, pipe, monitoring_row_details, andes_badge__content, monitoring_row_details__driver_tooltip__title, monitoring_row_details__driver_tooltip__metrics_stat, monitoring_row_details__driver_tooltip__metrics_stat_2, andes_badge__content_2, monitoring_row_details__untracked_2, menu__button_src, monitoring_row_details__license_3, pipe_3,latitud,longitud)
            VALUES %s
            """
            values = [
                (id_usuario, ids_usuario, fecha,item.get('monitoring-row__bold', None),item.get('monitoring-row-details__driver-name', None),item.get('sc-progress-wheel__percentage', None), item.get('monitoring-row-shipments__delivered-packages-text 2', None),
                item.get('monitoring-row-shipments__delivered-packages-text 3', None), verify_int(item.get('monitoring-row-shipments__delivered-packages-text 4', None), None ),item.get('monitoring-row-shipments__packages 2', None),item.get('andes-visually-hidden 2', None),
                item.get('metric-box__value-principal', None),item.get('metric-box__value-principal 2', None),item.get('metric-box__value-principal 3', None),item.get('metric-box__value-principal 4', None),item.get('metric-box__value-principal 5', None), item.get('metric-box__value-principal 6', None),
                item.get('andes-visually-hidden 4', None),item.get('metric-box__value-principal 7', None),item.get('monitoring-row-details__name', None),item.get('monitoring-row-details__untracked', None),item.get('monitoring-row__chevron--open src', None),
                item.get('monitoring-row-details__license', None),item.get('pipe', None),item.get('monitoring-row-details', None),item.get('andes-badge__content', None),item.get('monitoring-row-details__driver-tooltip__title', None),item.get('monitoring-row-details__driver-tooltip__metrics-stat', None),
                item.get('monitoring-row-details__driver-tooltip__metrics-stat 2', None),item.get('andes-badge__content 2', None),item.get('monitoring-row-details__untracked 2', None),
                item.get('menu__button src', None),item.get('monitoring-row-details__license 3', None),item.get('pipe 3', None),latitud, longitud
                )
                for item in body
            ]
            execute_values(cur, query, values)

        self.conn.commit()
    
    #### insert de datos prefactura con total
        
    def insert_datos_excel_prefactura_meli(self, id_usuario : int,ids_usuario : str,id_prefact :int,periodo : str,body):

        with self.conn.cursor() as cur:

            # Truncar la tabla antes de insertar los datos
            truncate_query = "TRUNCATE TABLE mercadolibre.prefactura_paso;"
            cur.execute(truncate_query)

            query = """
            INSERT INTO mercadolibre.prefactura_paso
            (id_usuario, ids_usuario, id_prefactura, periodo, descripcion, id_de_ruta, fecha_de_inicio, fecha_de_fin, patente, conductor, cantidad, precio_unitario)
            VALUES %s
            """
            values = [
                (id_usuario, ids_usuario, id_prefact, periodo, item['DescripciÃ³n'], item['ID de ruta'], item['Fecha de inicio'], item['Fecha de fin'], item['Patente'], item['Conductor'], item['Cantidad'], item['Precio unitario'])
                for item in body
            ]
            execute_values(cur, query, values)

        self.conn.commit()
    #### insert de datos prefactura sin total

    def insert_datos_excel_prefactura_meli_minus(self, id_usuario : int,ids_usuario : str,id_prefact :int,periodo : str,body):

        with self.conn.cursor() as cur:
            query = """
            INSERT INTO mercadolibre.prefactura_paso
            (id_usuario, ids_usuario, id_prefactura, periodo, descripcion, id_de_ruta, fecha_de_inicio, fecha_de_fin, patente, conductor, cantidad, precio_unitario)
            VALUES %s
            """
            values = [
                (id_usuario, ids_usuario, id_prefact, periodo, item['DescripciÃ³n'], item['ID de ruta'], item['Fecha de inicio'], item['Fecha de fin'], item['Patente'], item['Conductor'], item['Cantidad'], item['Precio unitario'])
                for item in body
            ]
            execute_values(cur, query, values)

        self.conn.commit()


    def insert_datos_excel_prefactura_mensual_meli(self, id_usuario : int,ids_usuario : str,body):

        with self.conn.cursor() as cur:
            query = """
             INSERT INTO mercadolibre.prefactura_paso
             (id_usuario, ids_usuario, id_prefactura, periodo, descripcion, id_de_ruta, fecha_de_inicio,
              fecha_de_fin, patente, conductor, cantidad, precio_unitario, total)
             VALUES %s
            """
            values = [
                (id_usuario, ids_usuario, item['ID prefactura'], item['Periodo'], item['Descripci√≥n'], item['ID de ruta'], item['Fecha de inicio'], 
                 item['Fecha de fin'], item['Patente'], item['Conductor'], item['Cantidad'], item['Precio unitario'], item['Total'])
                for item in body
            ]
            execute_values(cur, query, values)

        self.conn.commit()
    
    def obtener_datos_excel_prefactura_meli(self,ano,mes):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                          
           SELECT id_usuario, ids_usuario, id_prefactura, periodo, descripcion, id_de_ruta, 
            TO_CHAR(fecha_de_fin, 'YYYY-MM-DD') AS fecha_inicio, TO_CHAR(fecha_de_fin, 'YYYY-MM-DD') AS fecha_fin,
            patente, id_patente, conductor, cantidad, precio_unitario, descuento, total
            FROM mercadolibre.mae_proforma_mensual mpm
            WHERE SUBSTRING(mpm.periodo, 1, 4) = '{ano}' -- año
            AND SUBSTRING(mpm.periodo, 5, 2) = '{mes}';  -- mes
       
                         """)
            return cur.fetchall()
        
    def obtener_datos_excel_prefactura_meli_limit(self,ano,mes):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                          
           SELECT id_usuario, ids_usuario, id_prefactura, periodo, descripcion, id_de_ruta, 
            TO_CHAR(fecha_de_fin, 'YYYY-MM-DD') AS fecha_inicio, TO_CHAR(fecha_de_fin, 'YYYY-MM-DD') AS fecha_fin,
            patente, id_patente, conductor, cantidad, precio_unitario, descuento, total
            FROM mercadolibre.mae_proforma_mensual mpm
            WHERE SUBSTRING(mpm.periodo, 1, 4) = '{ano}' -- año
            AND SUBSTRING(mpm.periodo, 5, 2) = '{mes}' -- mes
            limit 300
       
                         """)
            return cur.fetchall()
        
    def obtener_datos_excel_prefactura_meli_descargar(self,ano,mes):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                          
           SELECT id_usuario, ids_usuario, id_prefactura, periodo, descripcion, id_de_ruta, 
            TO_CHAR(fecha_de_fin, 'YYYY-MM-DD') AS fecha_inicio, TO_CHAR(fecha_de_fin, 'YYYY-MM-DD') AS fecha_fin,
            patente, id_patente, conductor, cantidad, precio_unitario,
            case when descuento = true then 'x'
                else '' end as descuento,
            total
            FROM mercadolibre.mae_proforma_mensual mpm
            WHERE SUBSTRING(mpm.periodo, 1, 4) = '{ano}' -- año
            AND SUBSTRING(mpm.periodo, 5, 2) = '{mes}';  -- mes
       
                         """)
            return cur.fetchall()
        

    def ejecutar_funcion_tabla_paso_prefactura(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select * from mercadolibre.insertar_pedidos_mae_prefactura();
                         """)
            return cur.fetchone()
        

    
        

    ### Obtener Codigo de ambulancia
    def obtener_codigo_ambulancia(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select * from mercadolibre.genera_codigo_ambulancia();
                         """)
            return cur.fetchall()
        
    
    def retorno_ambulancia(self,op: int, cop: int,id_ppu: int, fecha: str):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                          
            select * from mercadolibre.retorno_ambulancia('{fecha}',{op},{cop},{id_ppu});
       
                         """)
            return cur.fetchall()  
    
    def obtener_estado_citacion_por_fecha_y_patente(self,id_ppu: int, fecha: str):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select tipo_ruta from mercadolibre.citacion c where fecha ='{fecha}' and id_ppu ='{id_ppu}
                         """)
            return cur.fetchall()


    def resumen_subida_archivo_prefactura(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select * from mercadolibre.resumen_proforma_manual();
                         """)
            return cur.fetchone()
        
    #### obtener datos DE MARCAS vehiculos

    def obtener_marcas_vehiculos(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select 
                json_agg(
                    json_build_object(
                        'id', id,
                        'name', marca
                    )
                ) AS result
            from transporte.marca_vehiculo mv 
                         """)
            return cur.fetchone()
        

    #### obtener datos citacion activa

    def recupera_data_por_citacion_activa(self,op: int,cop : int, fecha : str):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            -- select * from mercadolibre.recupera_data_por_citacion_activa_v2({op},{cop},'{fecha}');
            SELECT 
            json_agg(
                json_build_object(
                    'id_ty', id_ty, 'operacion', operacion, 'estado', estado, 'estado_correcto', estado_correcto,'ruta_meli', ruta_meli, 'id_ppu', id_ppu,'razon_id', razon_id,
                    'ppu', ppu,'patente_igual', patente_igual,'driver', driver,'driver_ok', driver_ok, 'p_avance', p_avance,'avance', avance,'campos_por_operacion', campos_por_operacion,
                    'tipo_vehiculo', tipo_vehiculo,'valor_ruta', valor_ruta, 'ruta_cerrada', ruta_cerrada, 'observacion', observacion , 'kilometro', km, 'tipo_ruta', tipo_ruta
              )) AS result

            from mercadolibre.recupera_data_por_citacion_activa_v2({op},{cop},'{fecha}');
                         """)
            return cur.fetchone()
        
    def recupera_data_por_citacion_supervisor(self,id_usuario: int, fecha : str):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --select id_operacion as "Id_operacion", nombre as "Nombre", id_centro_op as "Id_centro_op",
	        --       centro as  "Centro", region_name as "Region", detalles as "Detalles"     
            --from mercadolibre.pantalla_inicial_supervisores_v2('{fecha}', {id_usuario});
            SELECT json_agg(
                json_build_object(
                    'Id_operacion', id_operacion,
                    'Nombre', nombre,
                    'Id_centro_op', id_centro_op,
                    'Centro', centro,
                    'Region', region_name,
                    'Detalles', detalles
                )
            ) AS result
            FROM mercadolibre.pantalla_inicial_supervisores_v2('{fecha}', {id_usuario});
                         """)
            return cur.fetchone()
        

    def insert_bitacora_meli(self,id_usuario: int, ids_usuario:str, modificacion: str, latitud : float , longitud: float , origen):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            INSERT INTO mercadolibre.bitacora_general(id_usuario,ids_usuario, modificacion, latitud, longitud, origen)VALUES('{id_usuario}', '{ids_usuario}', '{modificacion}', '{latitud}', '{longitud}', '{origen}');                     
              """)
        self.conn.commit()


    def insert_datos_de_citacion_activa_FM(self,body,item):

        with self.conn.cursor() as cur:
            query = """
            INSERT INTO mercadolibre.mae_data_supervisores
            ( id_usuario, ids_usuario, latitud, longitud, operacion, id_operacion, id_centro_operacion, estado, fecha, nombre_ruta, tipo_ruta, id_ruta, p_avance, avance, 
            fm_total_paradas, fm_paqueteria_colectada, fm_estimados, fm_preparados, lm_fallido, lm_pendiente, lm_spr, lm_entregas, driver, fm_p_colectas_a_tiempo, 
            fm_p_no_colectadas, lm_tiempo_ruta, lm_estado, ppu, id_ppu, tipo_vehiculo, razon_id, valor_ruta, ruta_cerrada, estado_correcto, patente_igual, driver_ok,
            kilometros,observacion)
            VALUES %s
            """
            values = [
                # (
                #     body['id_usuario'], body['ids_usuario'], body['latitud'], body['longitud'], body['operacion'], body['id_operacion'], body['id_centro_operacion'],
                #     item['estado'], item['fecha'], item['nombre_ruta'], item['tipo_ruta'], item['id_ruta'], item['p_avance'], item['avance'], item['fm_total_paradas'], 
                #     item['fm_paqueteria_colectada'], item['fm_estimados'], item['fm_preparados'], item['lm_fallido'], item['lm_pendiente'], item['lm_spr'], 
                #     item['lm_entregas'], item['driver'], item['fm_p_colectas_a_tiempo'], item['fm_p_no_colectadas'], item['lm_tiempo_ruta'], item['lm_estado'], 
                #     item['ppu'], item['id_ppu'], item['tipo_vehiculo'], item['razon_id'], item['valor_ruta'], item['ruta_cerrada'], item['estado_correcto'], 
                #     item['patente_igual'], item['driver_ok']
                # )
                (
                    body.id_usuario, body.ids_usuario, body.latitud, body.longitud, body.operacion, body.id_operacion, body.id_centro_operacion,
                    item.estado, item.fecha, item.nombre_ruta, item.tipo_ruta, item.ruta_meli, item.p_avance, item.avance, item.fm_total_paradas, item.fm_paqueteria_colectada,
                    item.fm_estimados, item.fm_preparados, item.lm_fallido, item.lm_pendiente, item.lm_spr, item.lm_entregas, item.driver, item.fm_p_colectas_a_tiempo, 
                    item.fm_p_no_colectadas, item.lm_tiempo_ruta, item.lm_estado, item.ppu, item.id_ppu, item.tipo_vehiculo, item.razon_id, item.valor_ruta, item.ruta_cerrada, 
                    item.estado_correcto, item.patente_igual, item.driver_ok, item.kilometro, item.observacion
                )
                # for item in body.datos
            ]
            execute_values(cur, query, values)

            # print(values)

        self.conn.commit()

    
    def insert_datos_de_citacion_activa_FM_ambulancia(self,body,item,id_ambulancia):

        with self.conn.cursor() as cur:
            query = """
            INSERT INTO mercadolibre.mae_data_supervisores
            ( id_usuario, ids_usuario, latitud, longitud, operacion, id_operacion, id_centro_operacion, estado, fecha, nombre_ruta, tipo_ruta, id_ruta, p_avance, avance, 
            fm_total_paradas, fm_paqueteria_colectada, fm_estimados, fm_preparados, lm_fallido, lm_pendiente, lm_spr, lm_entregas, driver, fm_p_colectas_a_tiempo, 
            fm_p_no_colectadas, lm_tiempo_ruta, lm_estado, ppu, id_ppu, tipo_vehiculo, razon_id, valor_ruta, ruta_cerrada, estado_correcto, patente_igual, driver_ok,
            kilometros,observacion)
            VALUES %s
            """
            values = [
                # (
                #     body['id_usuario'], body['ids_usuario'], body['latitud'], body['longitud'], body['operacion'], body['id_operacion'], body['id_centro_operacion'],
                #     item['estado'], item['fecha'], item['nombre_ruta'], item['tipo_ruta'], item['id_ruta'], item['p_avance'], item['avance'], item['fm_total_paradas'], 
                #     item['fm_paqueteria_colectada'], item['fm_estimados'], item['fm_preparados'], item['lm_fallido'], item['lm_pendiente'], item['lm_spr'], 
                #     item['lm_entregas'], item['driver'], item['fm_p_colectas_a_tiempo'], item['fm_p_no_colectadas'], item['lm_tiempo_ruta'], item['lm_estado'], 
                #     item['ppu'], item['id_ppu'], item['tipo_vehiculo'], item['razon_id'], item['valor_ruta'], item['ruta_cerrada'], item['estado_correcto'], 
                #     item['patente_igual'], item['driver_ok']
                # )
                (
                    body.id_usuario, body.ids_usuario, body.latitud, body.longitud, body.operacion, body.id_operacion, body.id_centro_operacion,
                    item.estado, item.fecha, item.nombre_ruta, item.tipo_ruta,id_ambulancia, item.p_avance, item.avance, item.fm_total_paradas, item.fm_paqueteria_colectada,
                    item.fm_estimados, item.fm_preparados, item.lm_fallido, item.lm_pendiente, item.lm_spr, item.lm_entregas, item.driver, item.fm_p_colectas_a_tiempo, 
                    item.fm_p_no_colectadas, item.lm_tiempo_ruta, item.lm_estado, item.ppu, item.id_ppu, item.tipo_vehiculo, item.razon_id, item.valor_ruta, item.ruta_cerrada, 
                    item.estado_correcto, item.patente_igual, item.driver_ok, item.kilometro, item.observacion
                )
                # for item in body.datos
            ]
            execute_values(cur, query, values)

            # print(values)

        self.conn.commit()

    

    def update_datos_de_citacion_activa_FM(self,body,item):

    
        with self.conn.cursor() as cur:
            cur.execute(f"""
            UPDATE mercadolibre.mae_data_supervisores
            SET
                id_usuario = {to_sql_value(body.id_usuario)},
                ids_usuario = {to_sql_value(body.ids_usuario)},
                latitud = {to_sql_value(body.latitud)},
                longitud = {to_sql_value(body.longitud)},
                operacion = {to_sql_value(body.operacion)},
                id_operacion = {to_sql_value(body.id_operacion)},
                id_centro_operacion = {to_sql_value(body.id_centro_operacion)},
                estado = {to_sql_value(item.estado)},
                fecha = {to_sql_value(item.fecha)},
                nombre_ruta = {to_sql_value(item.nombre_ruta)},
                tipo_ruta = {to_sql_value(item.tipo_ruta)},
                p_avance = {to_sql_value(item.p_avance)},
                avance = {to_sql_value(item.avance)},
                fm_total_paradas = {to_sql_value(item.fm_total_paradas)},
                fm_paqueteria_colectada = {to_sql_value(item.fm_paqueteria_colectada)},
                fm_estimados = {to_sql_value(item.fm_estimados)},
                fm_preparados = {to_sql_value(item.fm_preparados)},
                lm_fallido = {to_sql_value(item.lm_fallido)},
                lm_pendiente = {to_sql_value(item.lm_pendiente)},
                lm_spr = {to_sql_value(item.lm_spr)},
                lm_entregas = {to_sql_value(item.lm_entregas)},
                driver = {to_sql_value(item.driver)},
                fm_p_colectas_a_tiempo = {to_sql_value(item.fm_p_colectas_a_tiempo)},
                fm_p_no_colectadas = {to_sql_value(item.fm_p_no_colectadas)},
                lm_tiempo_ruta = {to_sql_value(item.lm_tiempo_ruta)},
                lm_estado = {to_sql_value(item.lm_estado)},
                ppu = {to_sql_value(item.ppu)},
                id_ppu = {to_sql_value(item.id_ppu)},
                tipo_vehiculo = {to_sql_value(item.tipo_vehiculo)},
                razon_id = {to_sql_value(item.razon_id)},
                valor_ruta = {to_sql_value(item.valor_ruta)},
                ruta_cerrada = {to_sql_value(item.ruta_cerrada)},
                estado_correcto = {to_sql_value(item.estado_correcto)},
                patente_igual = {to_sql_value(item.patente_igual)},
                driver_ok = {to_sql_value(item.driver_ok)},
                kilometros = {to_sql_value(item.kilometro)},
                observacion = {to_sql_value(item.observacion
                )},
                ultima_actualizacion = CURRENT_DATE
            WHERE
                id_ruta = {to_sql_value(item.ruta_meli)};
            """)


        self.conn.commit()



    def verificar_id_ruta_existe(self,id_ruta):

        if id_ruta is None:
            return [0]

        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select count(*) from mercadolibre.mae_data_supervisores mds where id_ruta = {id_ruta} 
                         """)
            return cur.fetchone()


        


    def insert_datos_de_citacion_activa_FM_all(self,body):

        with self.conn.cursor() as cur:
            query = """
            INSERT INTO mercadolibre.mae_data_supervisores
            ( id_usuario, ids_usuario, latitud, longitud, operacion, id_operacion, id_centro_operacion, estado, fecha, nombre_ruta, tipo_ruta, id_ruta, p_avance, avance, fm_total_paradas, fm_paqueteria_colectada, fm_estimados, fm_preparados, lm_fallido, lm_pendiente, lm_spr, lm_entregas, driver, fm_p_colectas_a_tiempo, fm_p_no_colectadas, lm_tiempo_ruta, lm_estado, ppu, id_ppu, tipo_vehiculo, razon_id, valor_ruta, ruta_cerrada, estado_correcto, patente_igual, driver_ok)
            VALUES %s
            """
            values = [
                # (
                #     body['id_usuario'], body['ids_usuario'], body['latitud'], body['longitud'], body['operacion'], body['id_operacion'], body['id_centro_operacion'],
                #     item['estado'], item['fecha'], item['nombre_ruta'], item['tipo_ruta'], item['id_ruta'], item['p_avance'], item['avance'], item['fm_total_paradas'], 
                #     item['fm_paqueteria_colectada'], item['fm_estimados'], item['fm_preparados'], item['lm_fallido'], item['lm_pendiente'], item['lm_spr'], 
                #     item['lm_entregas'], item['driver'], item['fm_p_colectas_a_tiempo'], item['fm_p_no_colectadas'], item['lm_tiempo_ruta'], item['lm_estado'], 
                #     item['ppu'], item['id_ppu'], item['tipo_vehiculo'], item['razon_id'], item['valor_ruta'], item['ruta_cerrada'], item['estado_correcto'], 
                #     item['patente_igual'], item['driver_ok']
                # )
                (
                    body.id_usuario, body.ids_usuario, body.latitud, body.longitud, body.operacion, body.id_operacion, body.id_centro_operacion,
                    item.estado, item.fecha, item.nombre_ruta, item.tipo_ruta, item.ruta_meli, item.p_avance, item.avance, item.fm_total_paradas, item.fm_paqueteria_colectada,
                    item.fm_estimados, item.fm_preparados, item.lm_fallido, item.lm_pendiente, item.lm_spr, item.lm_entregas, item.driver, item.fm_p_colectas_a_tiempo, 
                    item.fm_p_no_colectadas, item.lm_tiempo_ruta, item.lm_estado, item.ppu, item.id_ppu, item.tipo_vehiculo, item.razon_id, item.valor_ruta, item.ruta_cerrada, 
                    item.estado_correcto, item.patente_igual, item.driver_ok
                )
                for item in body.datos
            ]
            execute_values(cur, query, values)

            print(values)

        self.conn.commit()



        #### Gestión GPS

    
    def obtener_informacion_gps(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select * from transporte.obtener_informacion_gps()
                        """)
            return cur.fetchall()
        
    def update_oc_instalacion_gps(self, oc_instalacion:str, id: int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            UPDATE transporte.gps SET  oc_instalacion='{oc_instalacion}' WHERE id='{id}'
                          """)
        self.conn.commit()

    def update_oc_baja_gps(self, oc_baja:str, id: int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           UPDATE transporte.gps SET  oc_baja='{oc_baja}' WHERE id='{id}'
                          """)
        self.conn.commit()

    def update_monto_gps(self, monto:str, id: int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           UPDATE transporte.gps SET  monto='{monto}' WHERE id='{id}'
                          """)
        self.conn.commit()


    def update_descontado_gps(self, descontado:bool, id: int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
          UPDATE transporte.gps SET  descontado={descontado} WHERE id='{id}'
                          """)
        self.conn.commit()

    def update_devuelto_gps(self, devuelto:bool, id: int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           UPDATE transporte.gps SET  devuelto={devuelto} WHERE id='{id}'
                          """)
        self.conn.commit()
    

    def listar_vehiculos_con_observaciones(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                select json_agg(
                    json_build_object(
                        'Codigo_retorno', codigo_retorno,
                        'Ppu', ppu,
                        'Razon_social', razon_social,
                        'Rut', rut,
                        'Celular', celular,
                        'Permiso_circulacion_fvenc', permiso_circulacion_fec_venc,
                        'Soap_fvenc', soap_fec_venc,
                        'Revision_tecnica_fvenc', revision_tecnica_fec_venc,
                        'Gps', gps
                    )
                ) AS result
            from transporte.listar_vehiculos_con_observaciones();
                        """)
            return cur.fetchone()
        
    def listar_vehiculos_con_observaciones_descarga(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                select 
                    ppu,
                    razon_social,
                    rut,
                    celular,
                    permiso_circulacion_fec_venc,
                    soap_fec_venc,
                    revision_tecnica_fec_venc,
                    case when gps = true then '✓'
                    else 'x' end as gps
                from transporte.listar_vehiculos_con_observaciones();
                        """)
            return cur.fetchall()
        


    ##### Tarifario


    def obtener_info_tarifario(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                select nombre, valor_inferior, valor_superior, unidad from finanzas.caracteristica_tarifa ct 
                        """)
            return cur.fetchall()


    def obtener_tipo_unidad(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                select * from finanzas.tipo_unidad tu 
                        """)
            return cur.fetchall()
        

    def agregar_nueva_tarifa(self, id_usuario:str, ids_usuario: str, nombre:str, valor_inferior:int, valor_superior:int, unidad:int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           INSERT INTO finanzas.caracteristica_tarifa 
           (id_usuario, ids_usuario, nombre, valor_inferior, valor_superior, unidad) 
           VALUES('{id_usuario}','{ids_usuario}','{nombre}',{valor_inferior},{valor_superior},{unidad})
                          """)
        self.conn.commit()


    ### Datos patentes + estrellas

    def obtener_patentes_disponibles_crv_crm(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                SELECT json_agg(
                        json_build_object(
                            'Patentes', patentes_out,
                            'Tipo', tipo_out,
                            'Razon_social', razon_social_out,
                            'Rutas', rutas_out,
                            'Porcentaje', porcentaje_out,
                            'Estrellas', estrellas_out,
                            'Habilitado', habilitado_out
                        )
                    ) AS result
                    from rutas.obtener_patentes_disponibles_crv_crm();
                        """)
            return cur.fetchone()


### TOC

    def recuperar_clientes_transyanez(self):
                with self.conn.cursor() as cur:
                    cur.execute(f"""   
                with clientes_ty as (
                    select c.id as "Id_cliente", c.nombre as "Nombre_cliente"
                    from rutas.clientes c 
                    where c.activo = true
                )

                select json_agg(clientes_ty) from clientes_ty     
                                """)
                    return cur.fetchone()

    def recupera_productos_adelanto(self):
            with self.conn.cursor() as cur:
                cur.execute(f"""   
            SELECT 
                    json_agg(
                        json_build_object(
                            'Nombre', nombre,
                            'Direccion', direccion,
                            'Ciudad', ciudad,
                            'Telefono', telefono,
                            'entrega', entrega,
                            'Descripcion', descripcion,
                            'Fecha_comp_original', fecha_comp_original,
                            'Fecha_reprogramada', fecha_reprogramada,
                            'Region', region,
                            'Observacion', observacion_out
                )) AS result
                from rutas.recupera_productos_adelanto();         
                            """)
                return cur.fetchone()
            


    def reporte_razon_soc_at(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select * from transporte.reporte_razon_soc_at();

                                  
                         """)
            return cur.fetchall()


    def reporte_vehiculos_at(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select * from transporte.reporte_vehiculo_at();         
                         """)
            return cur.fetchall()


    def panel_colaboradores(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select titulo,cant from transporte.panel_colaboradores();        
                         """)
            return cur.fetchall()
        
    def panel_vehiculos(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select campo,cant from transporte.panel_vehiculos();        
                         """)
            return cur.fetchall()
        
    
    def panel_vehiculos_observados(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select campo,cant from transporte.panel_vehiculos_observados();      
                         """)
            return cur.fetchall()
        
    def panel_triplulacion(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select titulo,cant from transporte.panel_usuarios();    
                         """)
            return cur.fetchall()


    def datos_seleccionables_reclutamiento(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select 'Operacion' as nombre,
                json_agg(json_build_object('Id',id,'Operacion', nombre)) as campo
                from operacion.modalidad_operacion
                union all
                select 'Origen' as nombre,
                json_agg(json_build_object('Id',id,'Origen', origen)) as campo
                from transporte.origen_contacto
                union all
                select  'Estado_contacto' as nombre,
                json_agg(json_build_object('Id',id,'Estado_contacto', estado )) as campo
                from transporte.estados_contacto
                union all
                select 'Motivo_subestado' as nombre,
                json_agg(json_build_object('Id',id,'Motivo_subestado', motivo  )) as campo
                from transporte.motivo_subestado  
                union all 
                select 'Contacto_ejecutivo' as nombre,
                json_agg(json_build_object('Id',id,'Ejecutivo', nombre  )) as campo
                from hela.usuarios u 
                where rol_id  in ('70','71','72','73') and activate = true
                union all 
                select 'Tipo_vehiculo' as nombre, json_agg(json_build_object('Id',id,'Tipo_vehiculo', tipo )) as campo
                from transporte.tipo_vehiculo
                union all
                select 'Region' as nombre, json_agg(json_build_object('Id_region',id_region,'Nombre_region', region_name )) as campo
                from public.op_regiones
                union all
                select 'Comuna' as nombre, json_agg(json_build_object('Nombre_comuna',comuna_name,'Id_region', id_region,'Id_comuna', id_comuna  )order by id_comuna) as campo
                from public.op_comunas oc 
                union all
                select 'Comentarios' as nombre, json_agg(json_build_object('Id',id ,'Calificacion',calificacion ,'Icono', icono ,'Color', color,'Latitud', '','Longitud', '','Comentario', '')) as campo 
				from transporte.experiencia_comentario;
                         """)
            return cur.fetchall()
        
    def datos_seleccionables_tripulacion(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select 'Comuna' as nombre, json_agg(json_build_object('Nombre_comuna',comuna_name,'Id_region', id_region,'Id_comuna', id_comuna  )order by id_comuna) as campo
            from public.op_comunas oc 
            union all
            select 'Marca_vehiculo' as nombre, json_agg(json_build_object('id', id,'name', marca) ORDER BY id ) AS campo
            from transporte.marca_vehiculo mv 
            union all 
            select 'Tipo_tripulacion' as nombre, json_agg(json_build_object('Id',id,'Tripulacion',tripulacion  ) ORDER BY id) as campo
            from transporte.tipo_tipulacion tt 
            union all
            select 'Region' as nombre, json_agg(json_build_object('Id_region',id_region,'Nombre_region', region_name ) ORDER BY id_region) as campo
            from public.op_regiones
                         """)
            return cur.fetchall()
        

    def datos_vehiculos(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select 'Comuna' as nombre, json_agg(json_build_object('Nombre_comuna',comuna_name,'Id_region', id_region,'Id_comuna', id_comuna  )order by id_comuna) as campo
                from public.op_comunas oc 
            union all
            select 'Marca_vehiculo' as nombre, json_agg(json_build_object('id', id,'name', marca) ORDER BY id ) AS campo
            from transporte.marca_vehiculo mv 
            union all 
            select 'Tipo_vehiculo' as nombre, json_agg(json_build_object('id',id,'name', tipo ) ORDER BY id) as campo
            from transporte.tipo_vehiculo
            union all
            select 'Region' as nombre, json_agg(json_build_object('Id_region',id_region,'Nombre_region', region_name ) ORDER BY id_region) as campo
            from public.op_regiones
            union all
            select 'Vehiculos_observaciones' as nombre, json_agg(
                json_build_object(
                    'Codigo_retorno', codigo_retorno,
                    'Ppu', ppu,
                    'Razon_social', razon_social,
                    'Rut', rut,
                    'Celular', celular,
                    'Permiso_circulacion_fvenc', permiso_circulacion_fec_venc,
                    'Soap_fvenc', soap_fec_venc,
                    'Revision_tecnica_fvenc', revision_tecnica_fec_venc,
                    'Gps', gps
                )
            ) AS result
            from transporte.listar_vehiculos_con_observaciones();
                         """)
            return cur.fetchall()


    def datos_razon_social(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            SELECT 'Motivo' as nombre, json_agg(json_build_object('id',id,'motivo', motivo)order by id) as campo
                FROM transporte.motivo_desvinculacion
                union all
                SELECT 'Estado' as nombre, json_agg(json_build_object('Id',id,'Estado', estado)order by id) as campo
                FROM transporte.estado  
                union all    
                select 'Comuna' as nombre, json_agg(json_build_object('Nombre_comuna',comuna_name,'Id_region', id_region,'Id_comuna', id_comuna  )order by id_comuna) as campo
                from public.op_comunas oc 
                union all
                select 'Marca_vehiculo' as nombre, json_agg(json_build_object('id', id,'name', marca) ORDER BY id ) AS campo
                from transporte.marca_vehiculo mv 
                union all 
                select 'Tipo_vehiculo' as nombre, json_agg(json_build_object('id',id,'name', tipo ) ORDER BY id) as campo
                from transporte.tipo_vehiculo
                union all
                select 'Region' as nombre, json_agg(json_build_object('Id_region',id_region,'Nombre_region', region_name ) ORDER BY id_region) as campo
                from public.op_regiones
                         """)
            return cur.fetchall()
        
    
    def listar_drivers_con_observaciones(self):
        with self.conn.cursor() as cur:
                cur.execute(f"""   
                select 
                json_agg(json_build_object(
                    'Codigo_retorno',codigo_retorno,
                    'Nombre', nombre_completo,
                    'Rut', rut,
                    'Razon_social', razon_social,
                    'Telefono',telefono,
                    'Fec_venc_licencia',fec_venc_lic_conducir,
                    'Rut_valido',rut_valido) ) as campo
            
                from transporte.listar_drivers_con_observaciones();
                            """)
                return cur.fetchone()
        
    def insert_nuevo_candidato(self,data):
        with self.conn.cursor() as cur:

            cur.execute("""
                        
                INSERT INTO transporte.reclutamiento
                (id_user, ids_user, region, operacion_postula, nombre_contacto, telefono, tipo_vehiculo, origen_contacto, estado_contacto, motivo_subestado, 
                contacto_ejecutivo, razon_social, rut_empresa,capacidad,correo,comuna,cant_vehiculos,ppu, metros_cubicos,inicio_actividades_factura, giro)
                VALUES(%(Id_user)s, %(Ids_user)s, %(Region)s, %(Operacion_postula)s,%(Nombre_contacto)s, %(Telefono)s, %(Tipo_vehiculo)s, %(Origen_contacto)s, %(Estado_contacto)s,%(Motivo_subestado)s,
                       %(Contacto_ejecutivo)s,%(Razon_social)s,%(Rut_empresa)s,%(Capacidad)s,%(Correo)s,%(Comuna)s, %(Cant_vehiculos)s,%(Ppu)s,%(Metros_cubicos)s, %(Inicio_actividades_factura)s, %(Giro)s);
 
                 """,data)
            self.conn.commit()

    def update_candidato(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
              
                UPDATE transporte.reclutamiento
                SET last_update=CURRENT_TIMESTAMP, region=%(Region)s,comuna=%(Comuna)s ,operacion_postula=%(Operacion_postula)s, nombre_contacto=%(Nombre_contacto)s,
                telefono=%(Telefono)s, tipo_vehiculo=%(Tipo_vehiculo)s, 
                origen_contacto=%(Origen_contacto)s, estado_contacto=%(Estado_contacto)s, motivo_subestado=%(Motivo_subestado)s, 
                contacto_ejecutivo=%(Contacto_ejecutivo)s, razon_social=%(Razon_social)s, rut_empresa=%(Rut_empresa)s,
                cant_vehiculos=%(Cant_vehiculos)s, ppu=%(Ppu)s, metros_cubicos=%(Metros_cubicos)s, correo=%(Correo)s,
                capacidad=%(Capacidad)s , inicio_actividades_factura=%(Inicio_actividades_factura)s, giro=%(Giro)s
                WHERE id=%(Id_reclutamiento)s
                 """,data)
            self.conn.commit()

            
    def insert_comentario_reclutamiento(self,data):
        with self.conn.cursor() as cur:

            cur.execute("""
                        
                INSERT INTO transporte.reclutamiento_comentarios
                (id_user, ids_user, id_reclutamiento, latitud, longitud, comentario, estatus_comentario)
                VALUES(%(Id_user)s, %(Ids_user)s, %(Id_recluta)s, 
                %(Latitud)s,%(Longitud)s,%(Comentario)s,%(Id)s);
 
                 """,data)
            self.conn.commit()

    def obtener_modalidad_operaciones(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select id, nombre from operacion.modalidad_operacion mo    
                         """)
            return cur.fetchall()
        
    def obtener_info_tarifario_general_null(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            SELECT id,operacion, centro_operacion, tipo_vehiculo, capacidad, periodicidad, tarifa, fecha_de_caducidad FROM finanzas.tarifario_general tg WHERE fecha_de_caducidad IS NULL;
                         """)
            return cur.fetchall() 

    def obtener_centro_operacion(self,id_op):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select id,centro from operacion.centro_operacion co where id_op = {id_op}                      
              """)
            return cur.fetchall()
        
    
    def obtener_tipo_vehiculo(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select * from transporte.tipo_vehiculo tv                   
              """)
            return cur.fetchall()
        
    def obtener_caract_finanzas(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select id,nombre,valor_inferior,valor_superior,unidad from finanzas.caracteristica_tarifa ct                   
              """)
            return cur.fetchall()
    
    def obtener_periodicidad(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select * from finanzas.periodicidad p                   
              """)
            return cur.fetchall()
        

    def obtener_info_tarifario_general(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select * from finanzas.listar_tarifario_general();                            
                      """)
            return cur.fetchall() 
        
    
    def obtener_centro_operacion_filter(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select id, centro, descripcion from operacion.centro_operacion co                        
                      """)
            return cur.fetchall() 

    def actualizar_fecha_tarifario_general(self, id:str, fecha_de_caducidad:str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           UPDATE finanzas.tarifario_general SET fecha_de_caducidad='{fecha_de_caducidad}' WHERE id={id};
                          """)
        self.conn.commit()


    def agregar_nuevo_tarifario_general(self, id_usuario, ids_usuario, latitud, longitud, operacion, centro_operacion, tipo_vehiculo, capacidad, periodicidad, tarifa):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           INSERT INTO finanzas.tarifario_general(id_usuario, ids_usuario, latitud, longitud, operacion, centro_operacion, tipo_vehiculo, capacidad, periodicidad, tarifa) VALUES('{id_usuario}','{ids_usuario}','{latitud}','{longitud}',{operacion},{centro_operacion},{tipo_vehiculo},{capacidad},{periodicidad},{tarifa})
                          """)
        self.conn.commit()


    #### 

    


    def obtener_datos_reclutamiento(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           with reclutas as (
            SELECT distinct on (r.id)  r.id as  Id_reclutamiento, TO_CHAR(r.created_at, 'YYYY-MM-DD') as Fecha_creacion, coalesce(r.region,1) as Region,coalesce(r.comuna,1) as Comuna, r.operacion_postula as Operacion_postula,trim(r.nombre_contacto) as Nombre,
                    r.telefono as Telefono,r.tipo_vehiculo as Tipo_vehiculo,coalesce(r.origen_contacto, 4) as Origen_contacto,coalesce(r.estado_contacto, 3) as Estado_contacto,coalesce(r.motivo_subestado, 7) as Motivo_subestado, r.contacto_ejecutivo as Contacto_ejecutivo,
                    r.razon_social as Razon_social, r.rut_empresa as Rut_empresa,r.internalizado as Internalizado, re.region_name as Region_nombre,mo.nombre as Operacion_nombre,coalesce(oc.origen, 'Página web') as Nombre_origen , coalesce(ec.estado, 'En espera') as Nombre_estados,coalesce(mv.motivo, 'En espera') as Nombre_motivo,
                    u.nombre as Nombre_contacto, r.pais as Pais,coalesce(r.inicio_actividades_factura, false) as Inicio_actividades_factura,coalesce(r.giro, 1) as Giro,coalesce(r.cant_vehiculos,0) as Cantidad_vehiculo,
                    r.correo as Correo,r.ppu as Ppu, coalesce(r.metros_cubicos, 0) as Metros_cubicos,coalesce(r.capacidad, 0) as Capacidad,
                    CASE 
                        WHEN r.contacto_ejecutivo is not null THEN 1
                        WHEN r.created_at::date BETWEEN CURRENT_DATE - INTERVAL '6 days' AND CURRENT_DATE - INTERVAL '2 days' THEN 2
                        WHEN r.created_at::date <= CURRENT_DATE - INTERVAL '7 days' THEN 3
                    end as Rango_fecha,
                    r.pestana as Pestana,
                    rc.comentario as Comentario
            FROM transporte.reclutamiento r
            LEFT JOIN public.op_regiones re ON CAST(r.region AS varchar) = re.id_region
            LEFT JOIN transporte.origen_contacto oc ON r.origen_contacto = oc.id
            LEFT JOIN transporte.estados_contacto ec ON r.estado_contacto = ec.id
            LEFT JOIN transporte.motivo_subestado mv ON r.motivo_subestado = mv.id
            LEFT JOIN operacion.modalidad_operacion mo ON r.operacion_postula = mo.id
            LEFT JOIN hela.usuarios u ON r.contacto_ejecutivo = u.id
            left join transporte.reclutamiento_comentarios rc ON r.id = rc.id_reclutamiento
            
            )
            
            
            select json_agg(json_build_object(
                    'Id_reclutamiento', id_reclutamiento,
                    'Fecha_creacion', fecha_creacion,
                    'Region', region,
                    'Comuna', comuna,
                    'Operacion_postula', operacion_postula,
                    'Nombre', nombre,
                    'Telefono', telefono,
                    'Tipo_vehiculo', tipo_vehiculo,
                    'Origen_contacto', origen_contacto,
                    'Estado_contacto', estado_contacto,
                    'Motivo_subestado', motivo_subestado,
                    'Contacto_ejecutivo', contacto_ejecutivo,
                    'Razon_social', razon_social,
                    'Rut_empresa', rut_empresa,
                    'Internalizado', internalizado,
                    'Region_nombre', region_nombre,
                    'Operacion_nombre', operacion_nombre,
                    'Nombre_origen', nombre_origen,
                    'Nombre_estados', nombre_estados,
                    'Nombre_motivo',  nombre_motivo,
                    'Nombre_contacto', nombre_contacto,
                    'Pais', pais,
                    'Inicio_actividades_factura',inicio_actividades_factura,
                    'Giro',giro,
                    'Cantidad_vehiculo', cantidad_vehiculo,
                    'Correo', correo,
                    'Ppu', ppu,
                    'Metros_cubicos', metros_cubicos,
                    'Capacidad', capacidad,
                    'Rango_fecha', rango_fecha,
                    'Pestana', pestana,
                    'Comentario', comentario
                ))
            
            from reclutas 
                            
                      """)
            return cur.fetchone()


    def datos_experiencia_comentario(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select json_agg(json_build_object
                ('Id',id  ,
                'Calificacion',calificacion ,
                'Icono', icono ,
                'Color', color,
                'Latitud', '',
                'Longitud', '',
                'Comentario', ''
                ) ) as campo 
            from transporte.experiencia_comentario;   
                      """)
            return cur.fetchone()
        
    
    def lista_comentarios_recluta(self,id):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select 
            json_agg(json_build_object
                                ('Estatus_comentario',estatus_comentario ,
                                'Nombre',nombre,
                                'Fecha', fecha,
                                'Hora', hora,
                                'Comentario', Comentario
                                ) ) as campo

            from transporte.obtener_comentarios_reclutamiento({id});   
                      """)
            return cur.fetchone()
        

     ##############


    def lista_ppu_con_fotos(self,fecha_ini,fecha_fin):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
           --- with ppus as (
           --- select distinct ppu as ppu
           --- FROM mercadolibre.evidencia_diaria_fm edf
           --- where ppu != 'null'
           --- )
           --- select 
           --- json_agg(ppu) as campo
           --- FROM ppus 
                        


            with ppus as (
            select distinct id_ruta as id_ruta, ppu
			FROM mercadolibre.evidencia_diaria_fm edf
			where ppu != 'null'
			and created_at BETWEEN '{fecha_ini}'::DATE AND '{fecha_fin}'::DATE
            )
            select
            json_agg(json_build_object
                                ('Ppu',ppu ,
                                'Id_ruta',id_ruta
                                ) ) as campo
            FROM ppus 
                      """)
            return cur.fetchone()

    def resumen_rutas_fecha_sup(self,fecha_ini,fecha_fin,usuario):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            with resumen_sup as (
            SELECT  mo.modalidad,mo.nombre AS operacion,co.centro AS centro_operacion,r.region_name AS region,
                    mds.fecha,mds.id_ruta,mds.ppu,mds.driver,
                    mds.kilometros,mds.p_avance,mds.avance,
                    CASE 
                        WHEN (SELECT maux.modalidad FROM operacion.modalidad_operacion maux WHERE maux.id = mo.id) = 'FM' THEN
                            (SELECT array_agg(json_build_object('fm_total_paradas', daux.fm_total_paradas, 'fm_paqueteria_colectada', daux.fm_paqueteria_colectada, 'fm_estimados', daux.fm_estimados, 'fm_preparados', daux.fm_preparados,'fm_p_colectas_a_tiempo', daux.fm_p_colectas_a_tiempo,'fm_p_no_colectadas', daux.fm_p_no_colectadas))
                            FROM mercadolibre.mae_data_supervisores daux
                            WHERE daux.id = mds.id)
                        WHEN (SELECT maux.modalidad FROM operacion.modalidad_operacion maux WHERE maux.id = mo.id) = 'LM' THEN 
                            (SELECT array_agg(json_build_object('lm_fallido',  daux.lm_fallido, 'lm_pendiente', daux.lm_pendiente, 'lm_spr', daux.lm_spr, 'lm_entregas', daux.lm_entregas,'lm_tiempo_ruta', daux.lm_tiempo_ruta,'lm_estado', daux.lm_estado))
                            FROM mercadolibre.mae_data_supervisores daux
                            WHERE daux.id = mds.id)
                    END AS campos_por_operacion,
                    mds.valor_ruta,
                        mds.observacion,
                    mds.ruta_cerrada
                FROM mercadolibre.mae_data_supervisores mds 
                LEFT JOIN operacion.centro_operacion co ON (co.id = mds.id_centro_operacion AND co.id_op = mds.id_operacion)
                LEFT JOIN operacion.modalidad_operacion mo ON mo.id = mds.id_operacion
                LEFT JOIN public.op_regiones r ON r.id_region::INT8 = co.region
                LEFT JOIN mercadolibre.citacion c ON c.ruta_meli::INT8 = mds.id_ruta
                WHERE mds.fecha BETWEEN '{fecha_ini}'::DATE AND '{fecha_fin}'::DATE
                and ({usuario} = ANY(co.id_coordinador) or {usuario} in (select u.id from hela.usuarios u where u.rol_id in ('5','90','72')))
                ORDER BY 1 DESC, 5 asc

            )


            
        ---select * from mercadolibre.resumen_rutas_fecha_sup('20240901','20240930',158,0);   
            
        


        select json_agg(
                json_build_object(
                'Modalidad',modalidad,
                'Operacion', operacion,
                'Centro_operacion', centro_operacion,
                'Region', region,
                'Fecha', fecha,
                'Id_ruta', id_ruta,
                'Ppu', coalesce (ppu,'') ,
                'Driver', coalesce(driver,''),
                'Kilometros', kilometros,
                'P_avance', p_avance,
                'Avance', avance,
                'Campos_por_operacion', campos_por_operacion,
                'Valor_ruta', valor_ruta,
                'Ruta_cerrada', ruta_cerrada,
                'Observacion', observacion
                )
        ) as campo
            from resumen_sup                           
                      """)
            return cur.fetchone() 

    ##############

    def listar_rutas_meli(self,fecha_inicio,fecha_fin):
        with self.conn.cursor() as cur:
            cur.execute(f"""   

            --select * from finanzas.listar_rutas_meli_v2('20241202','20241202',0);

             with lista_rutas as (
                select 	subquery.fecha,
                        subquery.modalidad,
                        subquery.id_ruta,
                        subquery.centro,
                        subquery.ppu,
                        subquery.tipo_vehiculo,
                        subquery.estado_ruta,
                        subquery.driver,
                        subquery.tipo,
                        subquery.ruta_auxiliada,
                        subquery.id_ambulancia,
                        subquery.avance,
                        subquery.fallidos,
                        subquery.lm_entregas,
                        subquery.total_pedidos,
                        subquery.km,
                        subquery.peoneta,
                        subquery.valor_ruta,
                        subquery.observacion,
                        CASE 
                            WHEN subquery.total_pedidos = 0 THEN 0
                            ELSE ROUND(subquery.lm_entregas::NUMERIC / subquery.total_pedidos::NUMERIC,4)
                        END as ns,
                        subquery.razon_id,
                        subquery.razon_social,
                        subquery.rut_empresa,
                        subquery.en_proforma,
                        subquery.p_total_descuentos,
                        subquery.p_a_pago,
                        subquery.patente_proforma
                from (
                    SELECT 	distinct on (mds.id_ruta)
                            mds.fecha,
                            mo.modalidad,
                            mds.id_ruta,
                            co.centro,
                            mds.ppu,
                            mds.tipo_vehiculo,
                            CASE 
                                WHEN mds.ruta_cerrada = TRUE THEN 'Cerrada'
                                ELSE 'Abierta'
                            END AS estado_ruta,
                            mds.driver,
                            tr.tipo,
                            c.ruta_meli_amb AS ruta_auxiliada,
                            c.ruta_amb_interna as id_ambulancia,
                            mds.avance,
                            CASE 
                                WHEN mo.modalidad = 'LM' THEN (coalesce(mds.lm_fallido,0))
                                WHEN mo.modalidad = 'FM' THEN 0
                                ELSE 0
                            END as fallidos,
                            mds.lm_entregas,
                            CASE 
                                WHEN mo.modalidad = 'LM' THEN (coalesce(mds.lm_spr,0))
                                WHEN mo.modalidad = 'FM' THEN 0
                                ELSE 0
                            END as total_pedidos,
                            mds.kilometros AS km,
                            u.nombre_completo AS peoneta,
                            mds.valor_ruta,
                            mds.observacion,
                            mds.razon_id,
                            col.razon_social,
                            col.rut AS rut_empresa,
                            CASE 
                                WHEN mpm.id IS NOT NULL THEN TRUE 
                                ELSE FALSE 
                            END AS en_proforma,
                            finanzas.sumar_descuentos(mds.id_ruta) AS p_total_descuentos,
                            CASE 
                                    WHEN mpm.cantidad = 1 THEN TRUE 
                                    ELSE FALSE 
                            END AS p_a_pago,
                            mpm.patente as patente_proforma
                    FROM mercadolibre.mae_data_supervisores mds
                    LEFT JOIN operacion.centro_operacion co ON co.id = mds.id_centro_operacion
                    LEFT JOIN mercadolibre.citacion c ON (c.ruta_meli::int8 = mds.id_ruta)
                    LEFT JOIN mercadolibre.tipo_ruta tr ON tr.id = c.tipo_ruta
                    LEFT JOIN transporte.usuarios u ON u.id = c.id_peoneta
                    LEFT JOIN transporte.colaborador col ON col.id = mds.razon_id
                    LEFT JOIN operacion.modalidad_operacion mo ON mo.id = co.id_op
                    LEFT JOIN mercadolibre.mae_proforma_mensual mpm ON mpm.id_de_ruta = mds.id_ruta
                    WHERE mds.fecha >= '{fecha_inicio}'::date AND mds.fecha <= '{fecha_fin}'::date
                    --where mds.fecha >= '20241202'::date AND mds.fecha <= '20241202'::date
                ) subquery
                ORDER BY subquery.razon_id, subquery.ppu, subquery.fecha
                
                )
            
        select json_agg(
                        json_build_object(
                        'Fecha', fecha,
                        'Modalidad',modalidad,
                        'Id_ruta', id_ruta,
                        'Centro_operacion', centro,
                        'Ppu', ppu,
                        'Tipo_vehiculo', tipo_vehiculo,
                        'Estado_ruta', estado_ruta,
                        'Driver', coalesce(driver, ''),
                        'Tipo', tipo,
                        'Ruta_auxiliada', ruta_auxiliada,
                        'Id_ambulacia', id_ambulancia,
                        'Avance', avance,
                        'Lm_fallido', fallidos,
                        --'P_avance', p_avance,
                        'Lm_entregas', lm_entregas,
                        'Total_pedidos', total_pedidos,
                        'Km', km,
                        'Peoneta', peoneta,
                        'Valor_ruta', valor_ruta,
                        'Observacion', observacion,
                        'Ns', ns,
                        'Razon_id', razon_id,
                        'Razon_social', razon_social,
                        'Rut_empresa', rut_empresa,
                        'En_proforma', en_proforma,
                        'P_total_descuentos', p_total_descuentos,
                        'P_a_pago', p_a_pago,
                        'Patente_proforma', patente_proforma
                        )
                ) as campo
                    from lista_rutas   
                                    
                      """)
            return cur.fetchone() 



    ######### Tarifario Especifico

    def listar_tarifario_especifico(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select * from finanzas.listar_tarifario_especifico();
                      """)
            return cur.fetchall()
        
    def datos_cop_tarifario_especifico(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select id, centro, descripcion from operacion.centro_operacion co
                      """)
            return cur.fetchall()

    def actualizar_fecha_tarifario_especifico(self, id:str, fecha_de_caducidad:str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           UPDATE finanzas.tarifario_especifico SET fecha_de_caducidad='{fecha_de_caducidad}' WHERE id={id};
                          """)
        self.conn.commit()

    def insert_tarifario_especifico(self,id_usuario:str, ids_usuario:str, latitud:str, longitud:str, ppu:int, razon_social:int, operacion:int, centro_operacion:int, periodicidad: int, tarifa: int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           INSERT INTO finanzas.tarifario_especifico(id_usuario, ids_usuario, latitud, longitud, ppu, razon_social, operacion, centro_operacion, periodicidad, tarifa) VALUES('{id_usuario}','{ids_usuario}','{latitud}','{longitud}',{ppu},{razon_social},{operacion},{centro_operacion},{periodicidad},{tarifa})
                          """)
        self.conn.commit()

    def datos_op_tarifario_especifico(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select id, nombre from operacion.modalidad_operacion mo  
                      """)
            return cur.fetchall()
        

    def datos_cop_tarifario_especifico_por_id(self,id_op):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select id,centro from operacion.centro_operacion co where id_op = {id_op}
                      """)
            return cur.fetchall()

    def datos_razon_social_tarifario_especifio(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select id, razon_social from transporte.colaborador c
                      """)
            return cur.fetchall()

            

    def datos_razon_social_tarifario_especifico(self,id):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            select id, ppu  from transporte.vehiculo v  where razon_id = {id}
                      """)
            return cur.fetchall()

    
    def datos_tarifario_especifico_fecha_null(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            SELECT id, operacion, centro_operacion, ppu, razon_social, periodicidad, tarifa, fecha_de_caducidad FROM finanzas.tarifario_especifico te WHERE fecha_de_caducidad IS NULL;
                      """)
            return cur.fetchall()


    def insert_recluta_externo(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO transporte.trabajemos
            (nombre_contacto, telefono, pais, correo, region, comuna, cant_vehiculos, ppu, tipo_vehiculo, 
            tipo_carroceria, tipo_adicionales, metros_cubicos, inicio_actividades_factura, giro)
            VALUES(%(Nombre_contacto)s, %(Telefono)s, %(Pais)s, %(Correo)s, %(Region)s,
                   %(Comuna)s,  %(Cant_vehiculos)s, %(Ppu)s, %(Tipo_vehiculo)s,  %(Tipo_carroceria)s,
                   %(Tipo_adicionales)s,  %(Metros_cubicos)s, %(Inicio_actividades_factura)s,  %(Giro)s);
           
                          """,data)
        self.conn.commit()

    def obtener_campos_recluta_externo(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select 'Comuna' as nombre, json_agg(json_build_object('Nombre_comuna',comuna_name,'Id_region', id_region,'Id_comuna', id_comuna  )order by id_comuna) as campo
            from public.op_comunas oc 
            union all
            select 'Tipo_vehiculo' as nombre, json_agg(json_build_object('Id',id,'Name', tipo ) ORDER BY id) as campo
            from transporte.tipo_vehiculo
            union all
            select 'Region' as nombre, json_agg(json_build_object('Id_region',id_region,'Nombre_region', region_name ) ORDER BY id_region) as campo
            from public.op_regiones
            union all
            select 'Giro_factura' as nombre, json_agg(json_build_object('Id', id,'Giro', giro) ORDER BY id ) AS campo
            from transporte.giro_factura
            union all
            select 'Paises' as nombre, json_agg(json_build_object('Id', id,'Pais', pais,'Cod_telefono',codigo_telefonico) ORDER BY id ) AS campo
            from public.paises_latinoamerica
            union all
            select 'Tipo_carroceria' as nombre, json_agg(json_build_object('Id', id,'Tipo', tipo) ORDER BY id ) AS campo
            from transporte.tipo_carroceria
            union all
            select 'Tipo_adicionales' as nombre, json_agg(json_build_object('Id', id,'Tipo', tipo) ORDER BY id ) AS campo
            from transporte.tipo_adicionales
           
                          """)
            return cur.fetchall()

    def insert_bitacora_tienda_toc(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO rutas.toc_bitacora_tienda_mae
(id_usuario, ids_usuario, driver, guia, cliente, estado, subestado, id_transyanez, ids_transyanez, observacion, codigo1)
VALUES(%(Id_usuario)s, %(Ids_usuario)s, %(Driver)s, %(Guia)s, %(Cliente)s,
        %(EstadoStr)s, %(SubestadoStr)s,%(Id_transyanez)s, %(Ids_transyanez)s,
        %(Observacion)s, %(Codigo1)s);            
                        """,data)
        self.conn.commit()


    def campos_bitacora_tienda_toc(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
             with clientes_ty as (
                select c.id as "Id", c.nombre as "Clientes"
                from rutas.clientes c 
                where c.activo = true
                union all
                select 0 as "Id", 'Retiro/Cliente' as "Clientes"
            )

            select 'Estados' as nombre, json_agg(json_build_object('Id_estado',estado ,'Descripcion',descripcion)) as campo 
                from areati.estado_entregas ee
                where estado not in  (3)
            union all
            select 'Subestados' as nombre, json_agg(json_build_object('Id_subestado',code ,'Id_estado',parent_code,'Descripcion',name)) as campo 
                            from areati.subestado_entregas se 
                            where parent_code in (1,2)                      
            union all
            select 'Codigo1' as nombre, json_agg(json_build_object('Id',id ,'Descripcion',descripcion)) as campo 
                            from rutas.def_codigo1
            union all
            select 'Clientes_ty' as nombre,json_agg(clientes_ty) from clientes_ty
            union all
            select 'Clientes' as nombre, json_agg(json_build_object('Id',id ,'Clientes',cliente)) as campo 
                from rutas.toc_clientes
                      """)
            return cur.fetchall()

    def panel_alertas_vigentes_toc(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
             select campo, cant from rutas.panel_toc_alertas();
                      """)
            return cur.fetchall()
        
    def datos_alertas_vigentes_toc(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
             select 'Comuna' as nombre, json_agg(json_build_object('Nombre_comuna',comuna_name,'Id_region', id_region,'Id_comuna', id_comuna  )order by id_comuna) as campo
                from public.op_comunas oc                
            union all
            select 'Subestados' as nombre, json_agg(json_build_object('Id',id ,'Parent_code',parent_code,'Nombre',name,'Codigo',code)) as campo 
                            from areati.subestado_entregas se 
                            where parent_code in (1,2)                      
            union all
            select 'Codigo1' as nombre, json_agg(json_build_object('Id',id ,'Descripcion',descripcion,'Descripcion_larga',descripcion_larga)) as campo 
                            from rutas.def_codigo1;
                      """)
            return cur.fetchall()
        

    ####Venta Traspaso

    def obtener_datos_PPU_ventas_traspaso(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
             select * from transporte.obtener_vehiculos_venta()
                      """)
            return cur.fetchall()
        
    def obtener_datos_Emp_ventas_traspaso(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
             SELECT * FROM transporte.obtener_colaboradores_venta();
                      """)
            return cur.fetchall()
        
    def cambiar_razon_social_vehiculo(self, razon_id: int, id : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           update transporte.vehiculo v set razon_id = '{razon_id}' where id ='{id}';
                          """)
        self.conn.commit()

    def ingreso_bitacora_cambio_razon_ppu(self, id_ppu: int, id_razon_Antigua: int, id_razon_nueva:int, observacion:str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           INSERT INTO transporte.cambio_razon_ppu(id_ppu, id_razon_antigua, id_razon_nueva, observacion) VALUES({id_ppu}, {id_razon_Antigua}, {id_razon_nueva}, '{observacion}');
                          """)
        self.conn.commit()


    def listar_bitacoras_tiendas_rango_fecha(self, fecha_inicio : str, fecha_fin : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             with listar_bitacora_tiendas_fechas as (
            select	to_char(tbtm.created_at,'yyyy-mm-dd hh24:mi') as fec_creacion, 
                    u.nombre as created_by,
                    tbtm.guia as guia,
                    tc.cliente as cliente,
                    tbtm.estado as estado,
                    tbtm.subestado as subestado,
                    tbtm.observacion as observacion,
                    tbtm.ids_transyanez  as codigo_ty
            from rutas.toc_bitacora_tienda_mae tbtm 
            left join rutas.toc_clientes tc on tc.id = tbtm.cliente
            left join hela.usuarios u on u.id = tbtm.id_usuario
            where to_char(tbtm.created_at,'yyyymmdd')>='{fecha_inicio}'
            and to_char(tbtm.created_at,'yyyymmdd')<='{fecha_fin}'
            order by tbtm.created_at desc 
            )

            select json_agg(json_build_object('Fecha_creacion',fec_creacion ,'Created_by',created_by,'Guia',guia,'Cliente',cliente,'Estado',estado,'Subestado',subestado,'Observacion',observacion,'Codigo_ty',codigo_ty)) as campo 
            from listar_bitacora_tiendas_fechas

                        """)
            return cur.fetchone()



    ### insertar datos en quadmind.pedidos_planificados v2
    @reconnect_if_closed_postgres
    def write_pedidos_planificados(self, data, posicion, direccion):
        # print(data)
        with self.conn.cursor() as cur: 
            consulta = f"""
            INSERT INTO quadminds.pedidos_planificados
            (cod_cliente, razon_social, domicilio, tipo_cliente, fecha_reparto, cod_reparto, maquina, chofer, fecha_pedido, 
            cod_pedido, cod_producto, producto, cantidad, horario, arribo, partida, peso, volumen, dinero,  posicion)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            # Ejecutar la consulta con los parámetros
            cur.execute(consulta, (
                data['cod_cliente'], data['razon_social'], direccion,
                data['tipo_cliente'], data['fecha_reparto'], data['cod_reparto'],
                data['nombre_ruta'], data['chofer'], data['fecha_pedido'], data['cod_pedido'],
                data['cod_producto'], data['producto'], data['cantidad'], data['horario'],
                data['arribo'], data['partida'], data['peso'], data['volumen'],
                data['dinero'], 
                # data['id_ruta_ty'], data['id_ruta_beetrack'], data['observacion'], 
                posicion
            ))

        self.conn.commit()


    def panel_citacion_meli(self,fecha):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
             select campo, cant from mercadolibre.panel_citacion('{fecha}');
                      """)
            return cur.fetchall()
        


    ## Finanzas descuentos

    def datos_seleccionables_descuentos(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            select 'Patentes' as nombre,
            json_agg(json_build_object('Id',id,'Patente', ppu)) as campo
            from transporte.vehiculo 
            union all
            select 'Razon_social' as nombre,
            json_agg(json_build_object('Id',id,'Nombre_razon', razon_social)) as campo
            from transporte.colaborador
            union all
            select  'Etiquetas' as nombre,
            json_agg(json_build_object('Id',id,'Etiqueta', etiqueta )) as campo
            from  finanzas.etiquetas_descuento_manual
                         """)
            return cur.fetchall()

    
    def datos_operacion_y_cop(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
            SELECT 
                mo.id as "Id_op",
                mo.nombre as "Operacion", 
                jsonb_agg(
                    jsonb_build_object(
                    'Nombre', co.centro,
                    'Id_cop', co.id
                    )
                ) AS centros
            FROM 
                operacion.centro_operacion co
            LEFT JOIN 
                operacion.modalidad_operacion mo 
                ON co.id_op = mo.id
            GROUP BY 
                mo.nombre, mo.id;
                         """)
            return cur.fetchall()


    def get_lista_descuentos(self,fecha_ini,fecha_fin):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            with descuentos as (

            SELECT 
                    dm.fecha_evento,
                    d.fecha_cobro,
                    u.nombre AS ingresado_por,
                    mo.modalidad AS operacion,
                    co.centro AS centro_operacion,
                    v.ppu,
                    c.razon_social,
                    d.numero_cuota || '/' || (SELECT COUNT(*) 
                                            FROM finanzas.descuentos aux 
                                            WHERE aux.id_origen_descuento = d.id_origen_descuento) AS cuota,
                    d.valor_cuota,
                    dm.monto AS total,
                    edm.etiqueta,
                    dm.descripcion,
                    dm.adjunto,
                    d.cobrada,
                    d.oc_cobro,
                    d.id,
                    dm.aplica,
                    d.id_origen_descuento
                FROM finanzas.descuentos d
                LEFT JOIN finanzas.descuentos_manuales dm ON dm.id = d.id_origen_descuento 
                LEFT JOIN operacion.modalidad_operacion mo ON mo.id = dm.id_operacion
                LEFT JOIN operacion.centro_operacion co ON co.id = dm.id_centro_op
                LEFT JOIN finanzas.etiquetas_descuento_manual edm ON edm.id = dm.etiqueta
                LEFT JOIN transporte.vehiculo v ON v.id = dm.id_ppu
                LEFT JOIN transporte.colaborador c ON c.id = dm.razon_social
                LEFT JOIN hela.usuarios u ON u.id = dm.id_user
                WHERE d.fecha_cobro BETWEEN '{fecha_ini}'::date AND '{fecha_fin}'::date
                ORDER BY d.fecha_cobro ASC

            )


            select 
            json_agg(json_build_object('Id',id,'Fecha_cobro',fecha_cobro,'Ingresado_por', ingresado_por,
                    'Operacion',operacion,'Centro_operacion',centro_operacion,'Ppu',ppu,
                    'Razon_social',razon_social,'Cuota',cuota,'Valor_cuenta',valor_cuota,
                    'Total',total,'Etiqueta',etiqueta,'Descripcion',descripcion,
                    'Cobrada',cobrada,'Oc_cobro',oc_cobro, 'Fecha_evento',fecha_evento,
                    'Adjunto',adjunto,'Aplica', aplica,'Id_desc_origen',id_origen_descuento)) as campo
            from descuentos

            """)

            return cur.fetchone()
        


    def agregar_archivo_adjunto_descuento(self,jpg, id):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE finanzas.descuentos_manuales
            SET adjunto='{jpg}'
            WHERE id = {id}
            """)
        self.conn.commit()


    def insert_descuentos_finanzas(self, body):

        with self.conn.cursor() as cur:
            cur.execute("""

            INSERT INTO finanzas.descuentos_manuales
            (id_user, ids_user, fecha_evento, id_ppu, razon_social, ruta, etiqueta, descripcion, monto, cuotas,id_operacion,id_centro_op)
            VALUES(%(Id_user)s, %(Ids_user)s, %(Fecha_evento)s, %(Pantente)s, %(Razon_social)s,
                    %(Ruta)s, %(Etiqueta)s,%(Descripcion)s, %(Monto)s, %(Cant_cuotas)s, %(Id_operacion)s, %(Id_cop)s);            
                        """,body)
            
        self.conn.commit()

    def update_descuentos_finanzas(self, oc_cobro, cobro, id):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            UPDATE finanzas.descuentos
            SET cobrada={cobro}, oc_cobro='{oc_cobro}'
            WHERE id={id}

                          """)
        self.conn.commit()


    ### actualizarAplica

    def update_aplica_descuentos(self,id, aplica):
        with self.conn.cursor() as cur:
            cur.execute(f"""
             UPDATE finanzas.descuentos_manuales dm 
            SET aplica = {aplica}
            WHERE id = {id}

                          """)
            
            row = cur.rowcount
            
        self.conn.commit()

        return row


    def get_max_id_descuentos_manuales(self) :
        with self.conn.cursor() as cur:
            cur.execute("""
            select coalesce (max(id)+1,1) from finanzas.descuentos_manuales dm 

            """)

            return cur.fetchone()
        
    def insert_datos_descuentos(self,body, id_origen : int,ids_origen : str):

       
        with self.conn.cursor() as cur:
            query = """
            INSERT INTO finanzas.descuentos
            (id_user, ids_user,  id_origen_descuento, ids_origen_descuento, fecha_cobro, valor_cuota, numero_cuota, origen, cobrada, oc_cobro)
            VALUES %s
            """
            values = [
                (
                    body['Id_user'], body['Ids_user'], id_origen, ids_origen, item['Fecha_comp'], item['Valor_cuota'], item['Numero_cuota'],
                    item['Origen'], item['Cobrada'], item['Oc_cobro']
                )
                for item in body['Cuotas']
            ]
            execute_values(cur, query, values)

            print(values)

        self.conn.commit()



    def insert_bitacora_finanzas(self,body):

       
        with self.conn.cursor() as cur:
            cur.execute("""

            INSERT INTO finanzas.bitacora_general
            (id_usuario, ids_usuario, modificacion, latitud, longitud, origen)
            VALUES(%(Id_user)s, %(Ids_user)s, %(Modificacion)s, %(Latitud)s, %(Longitud)s,
                    %(Origen)s);            
                        """,body)
            
        self.conn.commit()



    def armar_rutas_codigos_masivo(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
                select * from rutas.armar_ruta_codigos(%(Codigos)s, %(Fecha_ruta)s, %(Id_user)s)
                        """,data)
            return cur.fetchall()
        
    def get_fotos_patentes(self,ppu,id_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT  created_at, latitud, longitud, imagen1_png, imagen2_png, imagen3_png, imagen4_png
            FROM mercadolibre.evidencia_diaria_fm edf
            WHERE ppu = '{ppu}' and id_ruta = {id_ruta}
            ORDER BY created_at DESC
            LIMIT 1

            """)

            return cur.fetchone()
        

    ####  generador de codigos de ambulancias

    def get_max_id_meli_ambulancias(self) :
        with self.conn.cursor() as cur:
            cur.execute("""
            select * from mercadolibre.generar_codigo_ruta_ambulancia();

            """)

            return cur.fetchone()
        
    ###Task master

    def insert_activos_taskmaster(self, body):

        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO taskmaster.activos
            (id_user, id_area, categoria, nombre_equipo, marca, modelo, codigo, descripcion, region, comuna, direccion, 
            latitud, longitud, fecha_adquisicion, id_estado, garantia, proveedor, valor_adquisicion,
            vida_util, id_responsable,observaciones, activo, fecha_baja)
            VALUES(%(Id_user)s, %(Id_area)s, %(Categoria)s, %(Nombre_equipo)s, %(Marca)s, %(Modelo)s, %(Codigo)s, %(Descripcion)s, 
            %(Region)s, %(Comuna)s, %(Direccion)s, %(Latitud)s, %(Longitud)s, %(Fecha_adquisicion)s, %(Id_estado)s, 
            %(Garantia)s, %(Proveedor)s, %(Valor_adquisicion)s, %(Vida_util)s, %(Id_responsable)s, %(Observaciones)s, 
            %(Activo)s, %(Fecha_baja)s);           
                        """,body)
            
        self.conn.commit()


    def datos_seleccion_taskmasters(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select 'Responsables' as nombre,
            json_agg(json_build_object('Id',id,'Responsable', nombre)) as campo
            from hela.usuarios u where id not in (3,4,5,8,66,120,121) and u.activate = true
            union all   
            select 'Estados' as nombre,
            json_agg(json_build_object('Id',id,'Estado', nombre)) as campo
            from taskmaster.estados_activos
            union all
            select 'Areas' as nombre,
            json_agg(json_build_object('Id',id,'Area', nombre)) as campo
            from taskmaster.areas
            union all
            select 'Categorias' as nombre,
            json_agg(json_build_object('Id',id,'Categoria', nombre)) as campo
            from taskmaster.categorias
            union all
            select 'Comuna' as nombre, json_agg(json_build_object('Nombre_comuna',comuna_name,'Id_region', id_region,'Id_comuna', id_comuna  )order by id_comuna) as campo
            from public.op_comunas oc 
            union all
            select 'Region' as nombre, json_agg(json_build_object('Id_region',id_region,'Nombre_region', region_name ) ORDER BY id_region) as campo
            from public.op_regiones     
            union all       
            select 'Task_status' as nombre,
            json_agg(json_build_object('Id',id,'Status', name)) as campo
            from taskmaster.task_status;
                         """)
            return cur.fetchall()

    def get_max_id_activos(self) :
        with self.conn.cursor() as cur:
            cur.execute("""
            select coalesce (max(id)+1,1) from taskmaster.activos dm

            """)

            return cur.fetchone()


    def get_activos_taskmaster(self) :
        with self.conn.cursor() as cur:
            cur.execute("""
            with activos as (
                select 
                dm.id,
                dm.nombre_equipo,
                dm.categoria ,
                c.nombre,
                dm.activo
                from taskmaster.activos dm
                left join taskmaster.categorias c on c.id = dm.categoria 
            )

            select 
            json_agg(json_build_object('Id',id,'Nombre_equipo',nombre_equipo,'Categoria',categoria,
            'Nombre_categoria',nombre,'Activo', activo )) 
            from activos 

            """)

            return cur.fetchone()

    def actualizar_estados_activos(self, id):
        with self.conn.cursor() as cur:
            cur.execute(f""" 

            UPDATE taskmaster.activos
            SET activo = NOT activo
            WHERE id = {id};    
            """)

            row = cur.rowcount
        self.conn.commit()

        return row

    def agregar_archivo_adjunto_activo(self,pdf, id):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE taskmaster.activos
            SET manual_pdf='{pdf}'
            WHERE id = {id}
            """)
        self.conn.commit()


    def agregar_imagenes_activo(self,imagen1,imagen2,imagen3, id):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE taskmaster.activos
            SET imagen_1='{imagen1}', imagen_2='{imagen2}', imagen_3='{imagen3}'
            WHERE id = {id}
            """)
        self.conn.commit()


    #### Recupera_posibles supervisores


    def get_recupera_posibles_rutas(self, fecha_inicio : str, fecha_fin : str) :
        with self.conn.cursor() as cur:
            cur.execute(f"""
            ----select * from mercadolibre.resumen_rutas_fecha_sup('20250308','20250308',158,0);

            select
            json_agg(json_build_object('Id',id,'Created_at', created_at,'Ruta', ruta, 'Driver',driver,
            'Ppu',ppu,'Existe_citacion',existe_en_citacion,
            'Existe_en_mae_ds',existe_en_mae_data_supervisores,'En_proforma',en_proforma,
            'Usuarios',usuarios)) as campo
            from mercadolibre.recupera_posibles_rutas('{fecha_inicio}','{fecha_fin}',0);

            """)

            return cur.fetchone()


    #### Dispatch_paris


    def recuperar_ruta_registrada_paris(self,  id_despacho : str) :
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select distinct on (dp.route_id)
                dp.route_id
            from paris.dispatch_paris dp
            where dp.guide = '{id_despacho}'

            """)

            return cur.fetchone()


    def insert_dispatch_paris(self, body):

        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO paris.dispatch_paris
            (guide, identifier, route_id, dispatch_id, truck_identifier, 
            contact_name, contact_phone, contact_identifier, contact_email, contact_address,
            tag_asn_id, tag_desc_comuna, tag_desc_emp, tag_do_id, tag_fecdesfis, fechaemi, 
            fecsoldes, numcorhr, numsolgui, urlcarga, urlguia, item_id, item_name, 
            item_description, item_quantity, item_original_quantity, item_delivered_quantity, 
            item_code, item_carton, item_sku)
            VALUES(
                %(guide)s, %(identifier)s, %(route_id)s, %(dispatch_id)s, %(truck_identifier)s,  
                %(contact_name)s, %(contact_phone)s, %(contact_identifier)s, 
                %(contact_email)s, %(contact_address)s,
                %(tag_asn_id)s, %(tag_desc_comuna)s, %(tag_desc_emp)s, %(tag_do_id)s, 
                %(tag_fecdesfis)s, %(fechaemi)s, %(fecsoldes)s, %(numcorhr)s, 
                %(numsolgui)s, %(urlcarga)s, %(urlguia)s, %(item_id)s, %(item_name)s, 
                %(item_description)s, %(item_quantity)s, %(item_original_quantity)s, 
                %(item_delivered_quantity)s, %(item_code)s, %(item_carton)s, 
                %(item_sku)s);
        """, body)
            
        self.conn.commit()


    def insert_creacion_ruta_paris(self, body):

        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO paris.route
            (resource, evento, account_name, route, account_id, fecha, truck, 
            truck_driver, started, started_at, ended, ended_at)
            VALUES(
                %(resource)s, %(event)s, %(account_name)s, %(route)s, %(account_id)s,  
                %(date)s, %(truck)s, %(truck_driver)s, 
                %(started)s, %(started_at)s,
                %(ended)s, %(ended_at)s);
        """, body)
            
        self.conn.commit()

    
    def update_ruta_paris(self, body):

        with self.conn.cursor() as cur:
            cur.execute("""
            UPDATE paris.route
            SET resource= %(resource)s, evento= %(event)s, account_name=%(account_name)s,  account_id=%(account_id)s, fecha=%(date)s, truck=%(truck)s, 
                truck_driver=%(truck_driver)s, started=%(started)s, started_at=%(started_at)s, ended=%(ended)s, ended_at= %(ended_at)s
            WHERE route=%(route)s;
        """, body)
            
        self.conn.commit()



    ### Estado Paris

    def read_estados_paris(self, id_status, id_substatus, is_trunk,latitude, longitude):
            with self.conn.cursor() as cur:

                if is_trunk == True :

                    cur.execute(f"""  
                        select id_estado_destino as "status_id" , glosa_destino as "substatus", id_subestado_destino  as "substatus_code"
                    from paris.conversion_estados_beetrack ceb 
                    where id_subestado = {id_substatus}
                        """)
                else:
                    cur.execute(f"""  
                        select id_estado_destino as "status_id" , glosa_destino as "substatus", id_subestado_destino  as "substatus_code"
                    from paris.conversion_estados_beetrack ceb 
                    where id_subestado = {id_substatus}
                        """)

                
                
                return cur.fetchone()

        #### UPDATE RUTA PARIS

    def guardar_patente_paris(self, id_patente, ppu):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            INSERT INTO paris.patente_paris_beetrack
            (id_patente, ppu)
            VALUES({int(id_patente)}, '{ppu}');
        """)
            
        self.conn.commit()


    def read_route_paris(self, identifier):
        with self.conn.cursor() as cur:
            cur.execute(f"""  
                select distinct on(route_id) route_id from paris.dispatch_paris dp 
                where identifier= '{identifier}'
                limit 1
                """)
            return cur.fetchone()


    def guardar_informacion_de_rutas_paris(self, body):

        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO paris.ppu_tracking
            (ppu, id_route_ty, id_route_paris, is_trunk)
            VALUES(%(ppu)s, %(id_route_ty)s, %(id_route_paris)s, %(is_trunk)s);
                """,body)
            
        self.conn.commit()

    def verificar_informacion_ruta_paris(self, id_route_ty):
        with self.conn.cursor() as cur:
            cur.execute(f"""  
                select  id_route_ty, id_route_paris, COALESCE(evento, '') AS evento  from paris.ppu_tracking
                where id_route_ty = {id_route_ty} 
                order by created_at desc
                """)
            return cur.fetchone()
        

    def actualizar_estado_ruta_paris(self, id_route_ty, evento):
        with self.conn.cursor() as cur:
            cur.execute(f"""  
                UPDATE paris.ppu_tracking
                SET evento = '{evento}'
                WHERE id_route_ty = {id_route_ty}

                """)
        self.conn.commit()



    ##### obtener los campos las guias troncales


    def obtener_guias_troncales_paris(self, id_route_ty):
        with self.conn.cursor() as cur:
            cur.execute(f"""  
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'identifier', guia,
                        'status_id', 1,
                        'substatus', NULL
                    )
                ) AS resultados
                FROM beetrack.ruta_transyanez
                WHERE identificador_ruta = {id_route_ty} 
                AND cliente ILIKE '%paris%'
                AND is_trunk = false;
                """)
            return cur.fetchone()




    




    def get_cartones_despacho_paris(self, id_dispatch):
            with self.conn.cursor() as cur:
                cur.execute(f"""  
                     with cartones_id as (

                    select item_carton from paris.dispatch_paris
                    where guide = '{id_dispatch}'
                    )
                    select array_agg(item_carton) from cartones_id
                    """)
                return cur.fetchone()

    def update_campos_dispatch_paris(self, id_dispatch,estado,subestado):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            UPDATE paris.dispatch_paris AS tgt
            SET estado = {estado}, subestado = {subestado}
            WHERE dispatch_id = {id_dispatch}
            """)
            row = cur.rowcount
        self.conn.commit()

    
    def update_estado_dispatch_paris(self, id_dispatch,estado,subestado):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            UPDATE paris.dispatch_paris AS tgt
            SET estado = {estado}, subestado = {subestado}
            WHERE dispatch_id = {id_dispatch}
            """)
            row = cur.rowcount
        self.conn.commit()







    ### Dashboard pendientes
    def read_clientes_de_paris(self, body):
            with self.conn.cursor() as cur:
                cur.execute("""  
                    with guia_paris as (
                        SELECT  created_at, identificador_ruta, identificador, guia, cliente, lugar_de_trabajo, servicio, region_de_despacho, origen_de_la_entrega, fecha_estimada, fecha_llegada, estado, subestado, usuario_movil, telefono_usuario, id_cliente, nombre_cliente, direccion_cliente, telefono_cliente, correo_electronico_cliente, tiempo_en_destino, n_intentos, distancia_km, fechahr, tipo, email, conductor, fechaentrega, peso, cmn, cuenta, volumen, bultos, entrega, factura, oc, ruta, tienda, nombre_ejecutivo, codigo, observacion
                        FROM beetrack.ruta_transyanez
                        where guia = %(guide)s AND identificador_ruta = %(route_id)s
                        --where guia = '59634426' AND identificador_ruta = 43989886
                    )

                    select json_agg(guia_paris) from guia_paris
                    """,body)
                return cur.fetchone()


    #### guardar datos de clientes en tabla temporal beetrack

    def insert_tabla_temporal_ruta_beetrack(self,rutas, id_usuario : int,ids_usuario: str,cliente:str,id_cliente:int):

    
        with self.conn.cursor() as cur:
            query = """
            INSERT INTO beetrack.ruta_manual_transyanez_temp
            (id_user, ids_user, identificador_ruta, identificador, guia, lugar_de_trabajo, servicio, region_de_despacho, origen_de_la_entrega, 
            fecha_estimada, fecha_llegada, estado, subestado, usuario_movil, telefono_usuario, id_cliente, nombre_cliente, direccion_cliente, 
            telefono_cliente, correo_electronico_cliente, tiempo_en_destino, n_intentos, distancia_km, fechahr, tipo, email, conductor, 
            fechaentrega, peso, cmn, cuenta, volumen, bultos, entrega, factura, oc, ruta, tienda, nombre_ejecutivo, codigo, observacion, 
            valor_ruta,cliente_base,id_cliente_base)            
            
            VALUES %s
            """
            values = [
                (   id_usuario,ids_usuario,
                    body['id Ruta'], body['PPU'], body['N° Guía'], body['Lugar de Trabajo'], body['Servicio'], body['Región Despacho'],
                    body['Origen Entrega'], body['Fecha Estimada'], body['Fec. Llegada'], body['Estado '], body['Subestado'],
                    body['Driver'], body['telefono Driver'], body['rut Cliente'], body['Nombre Cliente'], body['Direccion Destino'],
                    body['teléfono Cliente'], body['Correo Cliente'], body['Tiempo en Destino'], body['N° Intentos'], body['Km'],
                    body['Fecha'], body['tipo'], body['e-mail'], body['conductor'], body['fecha Entrega'], body['peso'],
                    body['Comuna'], body['cuenta'], body['volumen'], body['bultos'], body['entrega'], body['factura'],
                    body['Orden Compra'], body['ruta'], body['tienda'], body['nombre Ejecutivo'], body['Codigo'],
                    body['observacion'], body['Valor Ruta'],cliente,id_cliente
                )
                for body in rutas
            ]
            execute_values(cur, query, values)

            print(values)

        self.conn.commit()


    
    def campos_de_carga_rutas_manuales(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
             with clientes_ty as (
                select c.id as "Id_cliente",c.nombre as "Cliente"
                from rutas.clientes c 
                where c.activo = true and c.carga_manual = true
            )

            select 'Clientes_ty' as nombre,json_agg(clientes_ty) from clientes_ty
                      """)
            return cur.fetchall()


    def obtener_lista_ids_rutas_y_pantes_temp(self,id_user:int):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
             with lista_ids as (

                    SELECT string_agg(id::text, ',') AS ids          
                    from beetrack.ruta_manual_transyanez_temp rmtt 
                    where rmtt.created_at::date = current_date and rmtt.id_user = {id_user}   
                    ---where rmtt.id_user = {id_user}   

                    )

                    SELECT json_agg(t) AS resultado
                    from(
                    SELECT 	id_salida as "Id",
                            identificador_ruta as "Ruta",
                            identificador as "Ppu",
                            guia as "Guia",
                            mensaje_ppu as "Mensaje_ppu",
                            proceder_ppu as "Proceder_ppu",
                            mensaje_ruta as "Mensaje_ruta",
                            proceder_ruta as "Proceder_ruta",
                            proceder as "Proceder"
                    FROM beetrack.fnc_valida_rutas_y_patentes_temp(
                    string_to_array((SELECT ids FROM lista_ids), ',')::int[]
                    )
                    ) t;
      
                      """)
            return cur.fetchone()
        

    #### Obtener lista de rutas manuales por bloques


    # def obtener_lista_ids_rutas_y_pantes_temp(self,id_user:int):
    #     with self.conn.cursor() as cur:
    #         cur.execute(f""" 
    #          with lista_ids as (

    #                 SELECT string_agg(id::text, ',') AS ids          
    #                 from beetrack.ruta_manual_transyanez_temp rmtt 
    #                 where rmtt.created_at::date = current_date and rmtt.id_user = {id_user}   
    #                 -----where rmtt.id_user = {id_user}   

    #                 )

    #                 SELECT json_agg(t) AS resultado
    #                 from(
    #                 SELECT 	id_salida as "Id",
    #                         identificador_ruta as "Ruta",
    #                         identificador as "Ppu",
    #                         guia as "Guia",
    #                         mensaje_ppu as "Mensaje_ppu",
    #                         proceder_ppu as "Proceder_ppu",
    #                         mensaje_ruta as "Mensaje_ruta",
    #                         proceder_ruta as "Proceder_ruta",
    #                         proceder as "Proceder"
    #                 FROM beetrack.fnc_valida_rutas_y_patentes_temp(
    #                 string_to_array((SELECT ids FROM lista_ids), ',')::int[]
    #                 )
    #                 ) t;
      
    #                   """)
    #         return cur.fetchone()
        

    #### Obtener lista de rutas manuales temporales


    def obtener_datos_rutas_y_pantes_temp(self,id_user):
        with self.conn.cursor() as cur:
            cur.execute(f""" 

            with lista_ids as (

                    SELECT string_agg(id::text, ',') AS ids          
                    from beetrack.ruta_manual_transyanez_temp rmtt 
                    where rmtt.created_at::date = current_date and rmtt.id_user = {id_user}   
                    --- where rmtt.id_user = {id_user}   

            )                


             SELECT 	id_salida as id,
                    identificador_ruta as ruta,
                    identificador as ppu,
                    guia as guia,
                    mensaje_ppu,
                    proceder_ppu,
                    mensaje_ruta,
                    proceder_ruta,
                    proceder
            FROM beetrack.fnc_valida_rutas_y_patentes_temp(string_to_array((SELECT ids FROM lista_ids), ',')::int[]);
                      """)
            return cur.fetchall()
        

    

     #### Procesar las rutas manuales temporales, pasandoles las ids de las rutas temporales

    def procesar_carga_manual(self, id_user):

        with self.conn.cursor() as cur:
            cur.execute(f""" 
                        
            with lista_ids as (

                    SELECT string_agg(id::text, ',') AS ids          
                    from beetrack.ruta_manual_transyanez_temp rmtt 
                    where rmtt.created_at::date = current_date and rmtt.id_user = {id_user}   
                    ----where rmtt.id_user = {id_user}   

            )       


            select * from beetrack.procesar_carga_manual(string_to_array((SELECT ids FROM lista_ids), ',')::int[]);
                      """)
            return cur.fetchone()
        


    #### eliminar las rutas manuales temporales,, pasandoles las ids de las rutas temporales como array

    def limpiar_ruta_manual_temp(self, id_user):

        with self.conn.cursor() as cur:
            cur.execute(f""" 
                        
            with lista_ids as (

                    SELECT string_agg(id::text, ',') AS ids          
                    from beetrack.ruta_manual_transyanez_temp rmtt 
                    where rmtt.created_at::date = current_date and rmtt.id_user = {id_user}   
                    ----where rmtt.id_user = {id_user}   

            )       
            select * from beetrack.limpiar_ruta_manual_temp(string_to_array((SELECT ids FROM lista_ids), ',')::int[]);
                      """)
            return cur.fetchone()



        #### obtener lista de rutas manuales por bloques
    
    def obtener_datos_rutas_manuales_por_bloque(self,fecha_ini,fecha_fin,bloque):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            SELECT json_agg(rutas) AS ruta_manual_bloques
                from
                (
                    select * from beetrack.fn_listar_ruta_manual_bloques('{fecha_ini}','{fecha_fin}',{bloque})
                ) as rutas
                      """)
            return cur.fetchone()
        


    def obtener_codigos_cartones_paris(self,cartones):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            with cartone as (
                select dp.item_carton, item_code,dp.item_id  from paris.dispatch_paris dp 
                where dp.item_carton  in ({cartones})
            )

            select json_agg(cartone) from cartone
                      """)
            return cur.fetchone()


    def update_valor_ruta_manual(self, ruta,guia,valor_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            UPDATE beetrack.ruta_manual_transyanez
            SET  valor_ruta={valor_ruta}
            WHERE identificador_ruta = {ruta} and guia = '{guia}'
            """)
            row = cur.rowcount
        self.conn.commit()

    
    def eliminar_ruta_manual(self, id_ruta,guia):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            DELETE FROM beetrack.ruta_manual_transyanez
            WHERE identificador_ruta = {id_ruta} and guia = '{guia}'
            """)
            row = cur.rowcount
        self.conn.commit()




class transyanezConnection():
    conn = None
    def __init__(self) -> None:
        try:
            self.conn = psycopg2.connect(config("POSTGRES_DB_TR"))

        except psycopg2.OperationalError as err:
            print(err)
            subprocess.run(comando, shell=False)
            # self.conn.close()
            # self.conn = psycopg2.connect(config("POSTGRES_DB_TR"))
    #Vehiculos Portal
    def get_vehiculos_portal(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            select
            --coalesce (TRIM(a.company_name) > '','-') as "Compañía",
            case when a.company_name <> '' then a.company_name
            else '(ID : ' || u.agencies[1] || ' )' -- || u.full_name
            end "Compañia",
            --u.agencies[1] || ' - ' || u.full_name as "ID - Nombre Colaborador",
            (select ra."name" from public.regions ra where ra.id =u.region) as "Región Origen",
            v.patent as "Patente",
            case
            when v.active then 'Activo'
            else 'Pendiente'
            end as "Estado",
            '(' || v.type || ') ' || vt."name" as "Tipo",
            -- v.carasteristic,
            vc."name" as "Caracteristica",
            --v.brand,
            vb."name" as "Marca",
            v.model as "Modelo",
            v.year as "Año",
            r."name" as "Region",
            c."name" as "Comuna"
            --v.agency_id,
            --v.driver_user_id,
            --v.peoneta_user_id
            --lower(u.mail) as "E-Mail"
            from "transyanez".vehicle v
            left join public.regions r on r.id=v.region
            left join public.communes c on c.id=v.commune
            left join public.agency a on a.id = v.agency_id
            left join "user".users u on cast(u.agencies[1] as integer) = cast(v.agency_id as integer)
            left join "transyanez".vehicle_type vt on vt.id = v."type"
            left join "transyanez".vehicle_characteristic vc on vc.id = v.carasteristic
            left join "transyanez".vehicle_brand vb on vb.id = v.brand
            where v.patent <> ''
            group by 1,2,3,4,5,6,7,8,9,10,11
            order by 1 asc
            """)
            return cur.fetchall()
        
