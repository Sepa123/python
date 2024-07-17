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
            SELECT id, full_name ,mail,"password" ,active ,rol_id, null as foto_perfil  FROM "user".users WHERE lower(mail) = lower(%(mail)s)
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
            select 1 as numero,  ruta_beetrack,patente,driver,inicio,region,total_pedidos,h1100_10,h1300_40,h1500_60,h1700_80,h1800_95,h2000_100,entregados,no_entregados,
                    case when estado_ruta = 'Finalizado' then 100
                        else 0
                    end
            from rutas.panel_resumen_rutas(); 
                
                        
            """)

            return cur.fetchall()
    
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
    @reconnect_if_closed_postgres
    def write_pedidos_planificados(self, data, posicion, direccion):
        # print(data)
        with self.conn.cursor() as cur: 
            consulta = f"""
            INSERT INTO quadminds.pedidos_planificados
            (cod_cliente, razon_social, domicilio, tipo_cliente, fecha_reparto, cod_reparto, maquina, chofer, fecha_pedido, 
            cod_pedido, cod_producto, producto, cantidad, horario, arribo, partida, peso, volumen, dinero, posicion)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            # Ejecutar la consulta con los parámetros
            cur.execute(consulta, (
                data['Código cliente'], data['Razón social'], direccion,
                data['Tipo de Cliente'], data['Fecha de Reparto'], data['Codigo Reparto'],
                data['Máquina'], data['Chofer'], data['Fecha De Pedido'], data['Codigo de Pedido'],
                data['Codigo de Producto'], data['Producto'], data['Cantidad'], data['Ventana Horaria'],
                data['Arribo'], data['Partida'], data['Peso (kg)'], data['Volumen (m3)'],
                data['Dinero ($)'], posicion
            ))

        self.conn.commit()

    
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
            select * from quadminds.convierte_en_ruta_manual({id}, '{fecha}');
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
             easygo.recepcion as "Recepcion"
        
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
                    easy.recepcion as "Recepcion"                 
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
             easygo.recepcion as "Recepcion"
             
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
                select * from areati.resumen_hora_productos_oc();
                        """)
            
            ### original mio
            ###No hay mucha diferencia

            # cur.execute("""
            #     with aux_opl as(
            #     select
            #     id_ruta,
            #     COUNT(DISTINCT o.id_entrega) AS "Entregas",
            #     COUNT(*) AS "Bultos",
            #     COUNT(*) FILTER (WHERE o.verified = true) AS "Verificados",
            #     COUNT(*) FILTER (WHERE o.verified = false) AS "Sin Verificar"
            #     FROM
            #     areati.ti_carga_easy_go_opl o
            #     group by 1
            #     ),
            #     aux_elux as (
            #         select e.ruta,
            #             count(distinct(e.numero_guia)) AS "Entregas",
            #             COUNT(*) AS "Bultos",
            #             COUNT(*) FILTER (WHERE e.verified = true) AS "Verificados",
            #             COUNT(*) FILTER (WHERE e.verified = false) AS "Sin Verificar"
            #         from areati.ti_wms_carga_electrolux e 
            #         group by 1
            #     ),
            #     aux_spo as (
            #     select
            #             COUNT(*) FILTER (WHERE s.verified = true) AS "Verificados",
            #             COUNT(*) FILTER (WHERE s.verified = false) AS "Sin Verificar"
            #     from areati.ti_wms_carga_sportex s
            #     where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            #     ),
            #     aux_cd as (
            #     select
            #         e.nro_carga,
            #         COUNT(DISTINCT e.entrega) AS "Entregas",
            #         COUNT(*) AS "Bultos",
            #         COUNT(*) FILTER (WHERE e.verified = true) AS "Verificados",
            #         COUNT(*) FILTER (WHERE e.verified = false) AS "Sin Verificar"
            #     FROM
            #         areati.ti_wms_carga_easy e
            #     group by 1
            #     )
                
            #     ----------------
            #     --aqui empieza--
            #     ----------------
                
            #     ---------------------------------------------------------------------------------------
            #     --  (1) Cuenta Easy CD
            #     ---------------------------------------------------------------------------------------
            #     select to_char(created_at,'yyyy-mm-dd') as "Fecha",
            #     to_char(created_at,'HH24:mi') as "Hora Ingreso", 
            #     'Easy CD' as "Cliente",
            #     easy.nro_carga as "N° Carga",
            #     acd."Entregas",
            #     acd."Bultos",
            #     acd."Verificados",
            #     acd."Sin Verificar"
            #     from areati.ti_wms_carga_easy easy 
            #     left join aux_cd acd on easy.nro_carga = acd.nro_carga
            #     WHERE to_char(created_at,'yyyy-mm-dd hh24:mi')  >= to_char((obtener_dia_anterior() + INTERVAL '17 hours 30 minutes'),'yyyy-mm-dd hh24:mi')
            #     AND to_char(created_at,'yyyy-mm-dd') <= to_char(CURRENT_DATE,'yyyy-mm-dd')
            #     group by 1,2,3,4,5,6,7,8
            #     ---------------------------------------------------------------------------------------
            #     --  (2) Cuenta Sportex
            #     ---------------------------------------------------------------------------------------
            #     union all
            #     select to_char(created_at,'yyyy-mm-dd') as "Fecha",
            #     to_char(created_at,'HH24:mi') as "Hora Ingreso", 
            #     'Sportex' as "Cliente",
            #     'Carga Unica' as "N° Carga",
            #     count(*) as "Entregas",
            #     count(*) as "Bultos",
            #     aspo."Verificados",
            #     aspo."Sin Verificar"
            #     --(select count(*) from areati.ti_wms_carga_sportex s where verified=true and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')) as "Verificados",
            #     --(select count(*) from areati.ti_wms_carga_sportex s where verified=false and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')) as "Sin Verificar"
            #     from areati.ti_wms_carga_sportex twcs
            #     left join aux_spo aspo on true
            #     where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            #     group by 1,2,3,4,7,8
            #     ---------------------------------------------------------------------------------------
            #     --  (3) Cuenta Electrolux
            #     ---------------------------------------------------------------------------------------
            #     union all
            #     select to_char(created_at,'yyyy-mm-dd') as "Fecha",
            #     to_char(created_at,'HH24:mi') as "Hora Ingreso",  
            #     'Electrolux' as "Cliente",
            #     twce.ruta as "N° Carga",
            #     aelux."Entregas",
            #     aelux."Bultos",
            #     aelux."Verificados",
            #     aelux."Sin Verificar"
            #     from areati.ti_wms_carga_electrolux twce 
            #     left join aux_elux aelux on twce.ruta = aelux.ruta
            #     where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            #     group by 1,2,3,4,5,6,7,8

            #     ---------------------------------------------------------------------------------------
            #     --  (4) Cuenta Easy OPL
            #     ---------------------------------------------------------------------------------------
            #     union all
            #     select to_char(opl.created_at,'yyyy-mm-dd') as "Fecha",
            #     to_char(opl.created_at,'HH24:mi') as "Hora Ingreso", 
            #     'Easy Tienda' as "Cliente",
            #     opl.id_ruta as "N° Carga",
            #     aopl."Entregas",
            #     aopl."Bultos",
            #     aopl."Verificados",
            #     aopl."Sin Verificar"
            #     from areati.ti_carga_easy_go_opl opl 
            #     left join aux_opl aopl on opl.id_ruta = aopl.id_ruta
            #     where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            #     group by 1,2,3,4,5,6,7,8
            #     ---------------------------------------------------------------------------------------
            #     -- Orden por Hora de ingreso de productos al sistema
            #     ---------------------------------------------------------------------------------------
            #     order by 1 asc, 2 asc
            #             """)
            

            return cur.fetchall()

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
        
    def buscar_producto_toc(self, codigo):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from rutas.toc_buscar_producto('{codigo}')
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
            select coalesce (max(id_transyanez)+1,1) from rutas.toc_bitacora_mae tbm 
                        """)
            return cur.fetchone() 

    def insert_bitacora_toc(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO rutas.toc_bitacora_mae
(fecha, ppu, guia, cliente, region, estado, subestado, driver, nombre_cliente, fec_compromiso, comuna, direccion_correcta, comuna_correcta, fec_reprogramada, observacion, subestado_esperado, id_transyanez, ids_transyanez, id_usuario, ids_usuario, alerta, codigo1)
VALUES( %(Fecha)s, %(PPU)s, %(Guia)s, %(Cliente)s, %(Region)s, %(Estado)s, %(Subestado)s, %(Driver)s, %(Nombre_cliente)s, %(Fecha_compromiso)s, %(Comuna)s, %(Direccion_correcta)s, %(Comuna_correcta)s
      , %(Fecha_reprogramada)s, %(Observacion)s, %(Subestado_esperado)s, %(Id_transyanez)s, %(Ids_transyanez)s, %(Id_usuario)s, %(Ids_usuario)s, %(Alerta)s, %(Codigo1)s);
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
                (identificador_ruta, identificador, guia, cliente, servicio, region_de_despacho , fecha_estimada, fecha_llegada, estado , usuario_movil, id_cliente, nombre_cliente  , direccion_cliente   , telefono_cliente , correo_electronico_cliente, fechahr    ,email, conductor, fechaentrega, cmn, volumen, bultos, factura, oc, ruta, tienda)
                VALUES( %(route_id)s, %(identifier)s, %(guide)s, %(Cliente)s,  %(Servicio)s, %(Región de despacho)s,  %(estimated_at)s, %(arrived_at)s, %(substatus)s, %(driver)s   ,  %(contact_identifier)s, %(contact_name)s,  %(contact_address)s, %(contact_phone)s, %(contact_email)s         , %(fechahr)s,%(contact_email)s, %(driver)s, %(fechaentrega)s, %(comuna)s, %(volumen)s, %(bultos)s, %(factura)s, %(oc)s, %(ruta)s, %(tienda)s);

                """,data)
            self.conn.commit()

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
                tienda = %(tienda)s
            WHERE guia = %(guide)s AND identificador_ruta = %(route_id)s;
            """, data)
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

    def fechas_pendientes(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select min(subquery.fec_min) as fec_min,
                    max(subquery.fec_max) as fec_max
                from (
                select 	min(easy.fecha_entrega) as fec_min,
                        max(easy.fecha_entrega) as fec_max
                from areati.ti_wms_carga_easy easy 
                WHERE (easy.estado = 0 OR (easy.estado = 2 AND easy.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND easy.estado NOT IN (1, 3)
                    and easy.entrega not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    and easy.entrega not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
                union all
                select 	min(eltx.fecha_min_entrega) as fec_min,
                        max(eltx.fecha_min_entrega) as fec_max
                from areati.ti_wms_carga_electrolux eltx
                WHERE (eltx.estado = 0 OR (eltx.estado = 2 AND eltx.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND eltx.estado NOT IN (1, 3)
                    and eltx.numero_guia not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    and eltx.numero_guia not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
                union all
                select  min(sptx.fecha_entrega) as fec_min,
                        max(sptx.fecha_entrega) as fech_max
                from areati.ti_wms_carga_sportex sptx
                WHERE (sptx.estado = 0 OR (sptx.estado = 2 AND sptx.subestado NOT IN (7, 10, 12, 13, 19, 43, 44, 50, 51, 70, 80)))
                    AND sptx.estado NOT IN (1, 3)
                    and sptx.id_sportex not in (select trb.guia from quadminds.ti_respuesta_beetrack trb)
                    and sptx.id_sportex not in (select drm.cod_pedido from quadminds.datos_ruta_manual drm where drm.estado=true)
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
                ) as subquery;
                        """)
            return cur.fetchone()
        
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
                    subquery.recepcion
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
                        sptx.recepcion    	
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
                        eltx.recepcion    	
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
    def pendientes_easy_opl_mio(self, fecha_inicio,fecha_fin, offset ):
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
            left join public.ti_tamano_sku tts on tts.sku = cast(easygo.codigo_sku as text)
            left join areati.estado_entregas ee on ee.estado=easygo.estado

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
       
    ## pendientes de de retiro tiendas
    def pendientes_retiro_tienda(self, fecha_inicio,fecha_fin, offset ):
         with self.conn.cursor() as cur:
            cur.execute(f"""
               --RETIRO cliente
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
                select distinct on (rtcl.cod_pedido)
                        rtcl.cod_pedido as guia,
                        'Envio/Retiro' as origen,
                        rtcl.created_at as fec_ingreso,
                        coalesce(tbm.fecha,rtcl.fecha_pedido) as fec_entrega,
                        rtcl.comuna as comuna,
                        rtcl.estado,
                        rtcl.subestado, 
                        rtcl.verified,
                        rtcl.verified as recepcion    	
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
         
      ## pendientes de Easy CD
    
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
                    subquery.recepcion
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
                        rtcl.verified as recepcion    	
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
           		coalesce(obs_total_pedidos,'No hay registros')
           	from rutas.seguimiento_transporte();
                        
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
                (id_user, ids_user, tipo_razon, razon_social, rut, celular,telefono, region, comuna, direccion, representante_legal, rut_representante_legal, email_rep_legal,chofer, peoneta, giro)
                VALUES(%(Id_user)s, %(Ids_user)s, %(Tipo_razon)s, %(Razon_social)s, %(Rut)s, %(Celular)s,
                        %(Telefono)s, %(Region)s, %(Comuna)s, %(Direccion)s, %(Representante_legal)s,
                         %(Rut_representante_legal)s, %(Email)s,false,false,%(Giro)s);                
    
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
                activo=%(Activo)s , giro=%(Giro)s, abogado=%(Abogado)s , seguridad=%(Seguridad)s,chofer=%(Chofer)s, peoneta=%(Peoneta)s
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
                crane_load_capacity_kg, permiso_circulacion_fec_venc, soap_fec_venc, revision_tecnica_fec_venc, validado_por_id, validado_por_ids, gps, gps_id)
                VALUES(%(Razon_id)s, %(Ppu)s, %(Marca)s, %(Tipo)s, %(Modelo)s, %(Ano)s, %(Region)s, %(Comuna)s, false, %(Activation_date)s, %(Capacidad_carga_kg)s, 
                        %(Capacidad_carga_m3)s, %(Platform_load_capacity_kg)s, %(Crane_load_capacity_kg)s, %(Permiso_circulacion_fec_venc)s, %(Soap_fec_venc)s, 
                        %(Revision_tecnica_fec_venc)s,  %(Id_user)s, %(Ids_user)s,%(Gps)s,%(Id_gps)s);               
    
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
                v.ano, coalesce (v.region, 0) , coalesce (v.comuna, 0), v.disponible, v.activation_date, v.capacidad_carga_kg, v.capacidad_carga_m3, 
                v.platform_load_capacity_kg, v.crane_load_capacity_kg, v.permiso_circulacion_fec_venc, v.soap_fec_venc, 
                v.revision_tecnica_fec_venc, v.registration_certificate, v.pdf_revision_tecnica, 
                v.pdf_soap, v.pdf_padron, v.pdf_gases_certification, v.validado_por_id, 
                v.validado_por_ids , c.razon_social, c.rut , v.gps, g.id , g.imei, g.fec_instalacion , g.oc_instalacion , v.habilitado, v.disponible 
            FROM transporte.vehiculo v
            left join transporte.colaborador c on v.razon_id = c.id    
            left join transporte.gps g on v.gps_id = g.id           
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
                    WHERE u.id_razon_social = col.id) AS usuarios
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
                v.ano, coalesce (v.region, 0) , coalesce (v.comuna, 0), v.disponible, v.activation_date, v.capacidad_carga_kg, v.capacidad_carga_m3, 
                v.platform_load_capacity_kg, v.crane_load_capacity_kg, v.permiso_circulacion_fec_venc, v.soap_fec_venc, 
                v.revision_tecnica_fec_venc, v.registration_certificate, v.pdf_revision_tecnica, 
                v.pdf_soap, v.pdf_padron, v.pdf_gases_certification, v.validado_por_id, 
                v.validado_por_ids , c.razon_social, c.rut , v.gps, g.id , g.imei, g.fec_instalacion , g.oc_instalacion , v.habilitado, v.disponible 
            FROM transporte.vehiculo v
            left join transporte.colaborador c on v.razon_id = c.id    
            left join transporte.gps g on v.gps_id = g.id    
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
                    WHERE u.id_razon_social = col.id) AS usuarios    
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

    def registrar_bitacora_transporte(self,data):
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
            WHERE id_razon_social = (select id from transporte.colaborador where rut = '{rut}' limit 1)
            """)
            self.conn.commit()

    def agregar_pdf_contrato(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE transporte.usuarios
            SET pdf_contrato='{pdf}'
            WHERE id_razon_social = (select id from transporte.colaborador where rut = '{rut}' limit 1)
            """)
            self.conn.commit()

    def agregar_pdf_cedula_identidad(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE transporte.usuarios
            SET pdf_cedula_identidad='{pdf}'
            WHERE id_razon_social = (select id from transporte.colaborador where rut = '{rut}' limit 1)
            """)
            self.conn.commit()

    def agregar_pdf_licencia_conducir(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE transporte.usuarios
            SET pdf_licencia_conducir='{pdf}'
            WHERE id_razon_social = (select id from transporte.colaborador where rut = '{rut}' limit 1)
            """)
            self.conn.commit()
    
    def agregar_pdf_antecedentes(self,pdf, rut):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
            --- PDF RRPP
            UPDATE transporte.usuarios
            SET pdf_antecedentes='{pdf}'
            WHERE id_razon_social = (select id from transporte.colaborador where rut = '{rut}' limit 1)
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
            (imei, fec_instalacion, oc_instalacion, id_user, ids_user)
            VALUES( %(Imei)s, %(Fecha_instalacion)s,%(Oc_instalacion)s,%(Id_user)s, %(Ids_user)s);

            """, data)
            self.conn.commit()

    ### actualiza datos si se actualizan los datos del gps
    def actualizar_datos_gps(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" 
                        
            UPDATE transporte.gps
            SET  imei= %(Imei)s, fec_instalacion=%(Fecha_instalacion)s, oc_instalacion=%(Oc_instalacion)s, 
                id_user=%(Id_user)s, ids_user=%(Ids_user)s
            WHERE id=%(Id_gps)s

            """, data)
            self.conn.commit()

 ### actualiza datos si se desactivan los datos del gps
    def actualizar_datos_gps_si_se_desactiva_gps(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" 
                        
            UPDATE transporte.gps
            SET imei= %(Imei)s, fec_baja=CURRENT_DATE, oc_baja=%(Oc_instalacion)s
            WHERE id=%(Id_gps)s

            """, data)
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
        where rol_id in ('80','81')
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
        
    def lista_conductores(self, fecha):
        with self.conn.cursor() as cur:
            cur.execute(f"""   
           select u.id, u.nombre_completo from transporte.usuarios u 
           where u.tipo_usuario = 1 AND u.id NOT IN 
           (select id_driver FROM mercadolibre.citacion c WHERE fecha = '{fecha}'::date and c.id_driver notnull);                       
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

    def update_estado_patente_citacion(self, estado: int, id_ppu : int, fecha:str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
UPDATE mercadolibre.citacion SET estado={estado} WHERE fecha='{fecha}' AND id_ppu={id_ppu}            
                        """)
        self.conn.commit() 

    def borrar_patente_citacion(self, id_ppu: str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            DELETE FROM mercadolibre.citacion WHERE id_ppu ='{id_ppu}'
                        """)
        self.conn.commit() 

    def update_estado_ruta_meli_citacion(self, ruta_meli: int, id_ppu : int, fecha: str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            UPDATE mercadolibre.citacion SET ruta_meli ={ruta_meli} WHERE id_ppu={id_ppu} and fecha='{fecha}'
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
        
    

    def update_ingresar_driver_peoneta(self, id_driver: int, id_peoneta : int, fecha: str, id_ppu:int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            UPDATE mercadolibre.citacion SET id_driver ={id_driver}, id_peoneta = {id_peoneta} WHERE fecha='{fecha}' AND id_ppu={id_ppu}
                      """)
        self.conn.commit()
        
    def update_tipo_ruta_citacion(self, tipo_ruta: int, id_ppu : int, fecha: str):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            UPDATE mercadolibre.citacion SET tipo_ruta ={tipo_ruta} WHERE id_ppu={id_ppu} and fecha='{fecha}'
                      """)
        self.conn.commit()

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
        


    
        
    def insert_datos_excel_prefactura_meli(self, id_usuario : int,ids_usuario : str,id_prefact :int,periodo : str,body):

        with self.conn.cursor() as cur:
            # cur.execute(f"""
            # INSERT INTO mercadolibre.prefactura_paso
            # (id_usuario, ids_usuario, id_prefactura, periodo, descripcion, id_de_ruta, fecha_de_inicio, fecha_de_fin, patente, conductor, cantidad, precio_unitario, total)
            # VALUES({id_usuario}, '{ids_usuario}',{id_prefact}, '{periodo}', '{body['DescripciÃ³n']}', '{body['ID de ruta']}', '{body['Fecha de inicio']}',
            #   '{body['Fecha de fin']}', '{body['Patente']}', '{body['Conductor']}', '{body['Cantidad']}', '{body['Precio unitario']}', '{body['Total']}')

            #           """)

            query = """
            INSERT INTO mercadolibre.prefactura_paso
            (id_usuario, ids_usuario, id_prefactura, periodo, descripcion, id_de_ruta, fecha_de_inicio, fecha_de_fin, patente, conductor, cantidad, precio_unitario, total)
            VALUES %s
            """
            values = [
                (id_usuario, ids_usuario, id_prefact, periodo, item['DescripciÃ³n'], item['ID de ruta'], item['Fecha de inicio'], item['Fecha de fin'], item['Patente'], item['Conductor'], item['Cantidad'], item['Precio unitario'], item['Total'])
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
    
    def obtener_datos_excel_prefactura_meli(self):
        with self.conn.cursor() as cur:
            cur.execute(f""" 
                          
           SELECT id, created_at, id_usuario, ids_usuario, id_prefactura, periodo, descripcion, id_de_ruta, 
            TO_CHAR(TO_TIMESTAMP(fecha_de_fin, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD') AS fecha_inicio, TO_CHAR(TO_TIMESTAMP(fecha_de_fin, 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD') AS fecha_fin,
            patente, conductor, cantidad, precio_unitario, total
            FROM mercadolibre.prefactura_paso;
       
                         """)
            return cur.fetchall()

        
        
        
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
        
