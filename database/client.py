import psycopg2
import codecs
from decouple import config
import os, sys, codecs

# import datetime
# import pytz


### Conexion usuario 

class UserConnection():
    conn = None
    def __init__(self) -> None:
        try:
            self.conn = psycopg2.connect(config("POSTGRES_DB_TR"))
        except psycopg2.OperationalError as err:
            print(err)
            print("Se conectara ???")
            self.conn.close()
            self.conn = psycopg2.connect(config("POSTGRES_DB_TR"))
        
    def __def__(self):
        self.conn.close()
    
    def conectar_bd(self):
        try:
            self.conn = psycopg2.connect(config("POSTGRES_DB_TR"))
        except psycopg2.OperationalError as err:
            print(err)
            print("Se conectara ???")
            self.conn.close()

    def write(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO public.users(username, password) VALUES(%(username)s, %(password)s);

            """,data)
        self.conn.commit()
    
    def read_all(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            select id, full_name ,mail,"password" ,active ,rol_id  from "user".users
            """)
            return cur.fetchall()
        
    def read_roles(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT id, "name", description, extra_data, is_sub_rol
                FROM "user".rol;
            """)
            return cur.fetchall()
    
    def get_nombre_usuario(self, id_usuario : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT full_name from "user".users
            where id = {id_usuario}
            """)
            return cur.fetchone()
        
    def read_only_one(self, data):
        # self.conn = self.conectar_bd()
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT id, full_name ,mail,"password" ,active ,rol_id  FROM "user".users WHERE mail=%(mail)s 
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
            self.conn.close()
            self.conn = psycopg2.connect(config("POSTGRES_DB_CARGA"), options=options)
            # self.conn.encoding("")
            self.conn.autocommit = True
            self.conn.set_client_encoding("UTF-8")
        
    def __def__(self):
        self.conn.close()

    def closeDB(self):
        self.conn.close()
    # Reporte historico 
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
                Select count(*) from areati.ti_wms_carga_sportex twcs
                Where to_char(twcs.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Sportex",
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
                Select count(*) from areati.ti_wms_carga_sportex twcs
                Where to_char(twcs.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Sportex",
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
                Select count(*) from areati.ti_wms_carga_sportex twcs
                Where to_char(twcs.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Sportex",
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
    def read_productos_sin_recepcion(self):
        with self.conn.cursor() as cur:
            cur.execute(
                """select * from hela.picking_anteriores();
                """
            )
            return cur.fetchall()

    
    #Cargas Verificadas y Total
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
                from generate_series(
                        current_date + '00:00:00'::time,
                            current_date + current_time,
                        '1 hour'::interval
                    ) AS intervalo
                union all
                select 'Total' as "Hora",
                (select count(distinct(numero_guia)) from areati.ti_wms_carga_electrolux twce3 where to_char(twce3.created_at,'yyyy-mm-dd') = to_char(current_date,'yyyy-mm-dd'))  as "Electrolux",
                (select count(*) from areati.ti_wms_carga_sportex twcs2 where to_char(twcs2.created_at,'yyyy-mm-dd') = to_char(current_date,'yyyy-mm-dd')) as "Sportex",
                (select count(distinct(entrega)) from areati.ti_wms_carga_easy twce2 where to_char(twce2.created_at,'yyyy-mm-dd') = to_char(current_date,'yyyy-mm-dd')) as "Easy CD",
                (select count(distinct(id_entrega)) from areati.ti_carga_easy_go_opl twce2 where to_char(twce2.created_at,'yyyy-mm-dd') = to_char(current_date,'yyyy-mm-dd')) as "Easy OPL"
                order by 1 desc
                ---------------------------------------------------------------------------------
            """)
            
            results = cur.fetchall()  
            return results
        
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
        
    def read_pedidos(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT SUM(t_ped) as "Total Pedidos",
                   SUM(t_ent) AS "Entregados",
                   SUM(n_ent) AS "No Entregados",
                   SUM(t_ped) - SUM(t_ent) - SUM(n_ent) AS "Pendientes"
            FROM areati.mae_ns_ruta_beetrack_hoy

            """)

            return cur.fetchall()
        
    def read_ruta_beetrack_hoy(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            select 
            ROW_NUMBER() OVER (ORDER BY id_ruta) as "N°",
            id_ruta as "Ruta",
            patente as "Patente",
            driver as "Driver", 
            h_inc as "Inicio",
            region as "Región",
            t_ped as "Total Pedidos",
            h1100 || ' (' || p10 ||'%)' as "11:00 (10%)",
            h1300 || ' (' || p40 ||'%)' as "13:00 (40%)",
            h1500 || ' (' || p60 ||'%)' as "15:00 (60%)",
            h1700 || ' (' || p80 ||'%)' as "17:00 (80%)",
            h1800 || ' (' || p95 ||'%)' as "18:00 (95%)",
            h2000 || ' (' || p100 ||'%)' as "20:00 (100%)",
            t_ent as "Entregados",
            n_ent as "No Entregados",
            p100 as "Porcentaje"
            from areati.mae_ns_ruta_beetrack_hoy 
            """)

            return cur.fetchall()
    
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

    def get_timezone(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            SHOW timezone
            """)
            return cur.fetchall()


    ## Pedidos Pendientes En ruta 

    def read_pedidos_pendientes_total(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            ---------------------------------------------------------------------------------------------------------
            -- Todos los Pedidos
            ---------------------------------------------------------------------------------------------------------
            WITH guias AS (
                select guia as busqueda
                from quadminds.ti_respuesta_beetrack
            ),
            fechas as(
            select fecha_entrega as fecha_compromiso
            from areati.ti_wms_carga_easy twce 
            where twce.entrega in (select busqueda from guias)
            union all
            select fecha_max_entrega as fecha_compromiso
            from areati.ti_wms_carga_electrolux twce
            where twce.numero_guia in (select busqueda from guias)
            union all
            select fecha_entrega as fecha_compromiso
            from areati.ti_wms_carga_sportex twcs 
            where twcs.id_sportex in (select busqueda from guias)
           -- union all
           -- select fec_compromiso as fecha_compromiso
           -- from areati.ti_carga_easy_go_opl tcego  
           -- where tcego.suborden in (select busqueda from guias)
            )
            SELECT 
                (SELECT COUNT(*) FROM fechas WHERE fecha_compromiso < CURRENT_DATE) AS "Atrasadas",
                (SELECT COUNT(*) FROM fechas WHERE fecha_compromiso = CURRENT_DATE) AS "En Fecha",
                (SELECT COUNT(*) FROM fechas WHERE fecha_compromiso > CURRENT_DATE) AS "Adelantadas"

            """)
            return cur.fetchall()
    
    def read_pedidos_pendientes_no_entregados(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            ---------------------------------------------------------------------------------------------------------
            -- Pedidos No Entregados
            ---------------------------------------------------------------------------------------------------------
            WITH guias AS (
                select guia as busqueda
                from quadminds.ti_respuesta_beetrack
                where lower(estado)='no entregado' -- Pendiente  No entregado
            ),
            fechas as(
            select fecha_entrega as fecha_compromiso
            from areati.ti_wms_carga_easy twce 
            where twce.entrega in (select busqueda from guias)
            union all
            select fecha_max_entrega as fecha_compromiso
            from areati.ti_wms_carga_electrolux twce
            where twce.numero_guia in (select busqueda from guias)
            union all
            select fecha_entrega as fecha_compromiso
            from areati.ti_wms_carga_sportex twcs 
            where twcs.id_sportex in (select busqueda from guias)
           -- union all
           -- select fec_compromiso as fecha_compromiso
            --from areati.ti_carga_easy_go_opl tcego  
          --  where tcego.suborden in (select busqueda from guias)
            )
            SELECT 
                (SELECT COUNT(*) FROM fechas WHERE fecha_compromiso < CURRENT_DATE) AS "Atrasadas",
                (SELECT COUNT(*) FROM fechas WHERE fecha_compromiso = CURRENT_DATE) AS "En Fecha",
                (SELECT COUNT(*) FROM fechas WHERE fecha_compromiso > CURRENT_DATE) AS "Adelantadas"

            """)
            return cur.fetchall()
        
    def read_pedidos_pendientes_en_ruta(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            ---------------------------------------------------------------------------------------------------------
            -- Pedidos En Ruta
            ---------------------------------------------------------------------------------------------------------
            WITH guias AS (
                select guia as busqueda
                from quadminds.ti_respuesta_beetrack
                where lower(estado)='pendiente' 
            ),
            fechas as(
            select fecha_entrega as fecha_compromiso
            from areati.ti_wms_carga_easy twce 
            where twce.entrega in (select busqueda from guias)
            union all
            select fecha_max_entrega as fecha_compromiso
            from areati.ti_wms_carga_electrolux twce
            where twce.numero_guia in (select busqueda from guias)
            union all
            select fecha_entrega as fecha_compromiso
            from areati.ti_wms_carga_sportex twcs 
            where twcs.id_sportex in (select busqueda from guias)
           -- union all
           -- select fec_compromiso as fecha_compromiso
           -- from areati.ti_carga_easy_go_opl tcego  
           -- where tcego.suborden in (select busqueda from guias)
            )
            SELECT 
                (SELECT COUNT(*) FROM fechas WHERE fecha_compromiso < CURRENT_DATE) AS "Atrasadas",
                (SELECT COUNT(*) FROM fechas WHERE fecha_compromiso = CURRENT_DATE) AS "En Fecha",
                (SELECT COUNT(*) FROM fechas WHERE fecha_compromiso > CURRENT_DATE) AS "Adelantadas"

            """)
            return cur.fetchall()

    def read_pedidos_pendientes_entregados(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            ---------------------------------------------------------------------------------------------------------
            -- Pedidos Entregados
            ---------------------------------------------------------------------------------------------------------
            WITH guias AS (
                select guia as busqueda
                from quadminds.ti_respuesta_beetrack
                where lower(estado)='entregado' -- Pendiente  No entregado
            ),
            fechas as(
            select fecha_entrega as fecha_compromiso
            from areati.ti_wms_carga_easy twce 
            where twce.entrega in (select busqueda from guias)
            union all
            select fecha_max_entrega as fecha_compromiso
            from areati.ti_wms_carga_electrolux twce
            where twce.numero_guia in (select busqueda from guias)
            union all
            select fecha_entrega as fecha_compromiso
            from areati.ti_wms_carga_sportex twcs 
            where twcs.id_sportex in (select busqueda from guias)
           -- union all
           -- select fec_compromiso as fecha_compromiso
           -- from areati.ti_carga_easy_go_opl tcego  
           -- where tcego.suborden in (select busqueda from guias)
            )
            SELECT 
                (SELECT COUNT(*) FROM fechas WHERE fecha_compromiso < CURRENT_DATE) AS "Atrasadas",
                (SELECT COUNT(*) FROM fechas WHERE fecha_compromiso = CURRENT_DATE) AS "En Fecha",
                (SELECT COUNT(*) FROM fechas WHERE fecha_compromiso > CURRENT_DATE) AS "Adelantadas"

            """)
            return cur.fetchall()


    ### Obtener productos

    def get_producto_picking(self):

        with self.conn.cursor() as cur:
            cur.execute("""
            select * from areati.buscar_producto_picking('2905254022');
            """)
            return cur.fetchone()
        
    def get_producto_picking_id(self, producto_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select areati.buscar_producto_picking('{producto_id}');
            """)
            return cur.fetchone()
        
    ## productos picking SKU

    def read_producto_sku(self,codigo_sku):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from areati.busca_producto_sku('{codigo_sku}')
            """)
            return cur.fetchall()

    ## Comparacion API VS WMS

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
        

    def write_producto_sin_clasificar(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO public.ti_tamano_sku (sku, descripcion, tamano, origen)
            VALUES(%(SKU)s, %(Descripcion)s, %(Talla)s, %(Origen)s);
            """,data)
        self.conn.commit()

    ### insertar datos en quadmind.pedidos_planificados

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



    def get_ruta_manual(self,pedido_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select * from areati.busca_ruta_manual('{pedido_id}')
            """)
            return cur.fetchall()
        
    def get_factura_electrolux(self,pedido_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select codigo_item , factura from areati.ti_wms_carga_electrolux eltx 
            where eltx.numero_guia = '{pedido_id}' or trim(eltx.factura) = trim('{pedido_id}')
            """)
            return cur.fetchall()
        
    def get_numero_guia_by_factura(self,factura):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select numero_guia from areati.ti_wms_carga_electrolux eltx 
            where trim(eltx.factura) = trim('{factura}')
            limit 1
            """)
            return cur.fetchone()
        
    def direccion_textual(self,pedido_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select "Dirección Textual" from areati.busca_ruta_manual('{pedido_id}')
            """)
            return cur.fetchall()
        
    def get_cod_producto_ruta_manual(self,pedido_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select "Código de Producto", "Descripción del Producto"  from areati.busca_ruta_manual('{pedido_id}')
            """)
            return cur.fetchall()

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
            "UPDATE areati.ti_wms_carga_sportex SET verified = true WHERE areati.ti_wms_carga_sportex.id_sportex = %s",
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

    ## Rutas en activo

    def read_rutas_en_activo(self,nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select ROW_NUMBER() over (ORDER BY id_ruta desc, posicion asc ) as "Pos.",* from
            (
            select cod_pedido as "Cod. Pedido",
            ciudad as "Comuna",
            string_agg(distinct(drm.sku) , '@') AS "SKU",
            string_agg(distinct(drm.desc_producto) , '@') AS "Producto",
            count(distinct(drm.sku)) as "UND",
            count(drm.sku) as "Bultos",
            initcap(nombre) as "Nombre Cliente",
            initcap(calle_numero) as "Direccion Cliente",
            telefono as "Telefono",
            estado as "Estado",
            '' as "Validado",
            CASE
                WHEN bool_or(de) THEN 'Embalaje con Daño'
                ELSE ''
            END AS "DE",
            CASE
                WHEN bool_or(dp) THEN 'Producto con Daño'
                ELSE ''
            END as "DP",
            provincia_estado,
            fecha_pedido,
            id_ruta,
            posicion
            from quadminds.datos_ruta_manual drm
            where nombre_ruta = '{nombre_ruta}'
            group by 1,2,7,8,9,10,11, id_ruta, posicion, provincia_estado, fecha_pedido
            ) datos_base;
            """)
            return cur.fetchall()
        
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

    def read_comunas_ruta_by_fecha(self,fecha):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select distinct(initcap(lower(ciudad)))
                from quadminds.datos_ruta_manual drm where TO_CHAR(fecha_ruta, 'YYYY-MM-DD') = '{fecha}'
                                    """)
            return cur.fetchall()
        
    def filter_nombres_rutas_by_comuna(self,fecha,comuna):
        with self.conn.cursor() as cur:
            cur.execute(f"""
        --    select distinct (nombre_ruta),estado
          -- from quadminds.datos_ruta_manual where TO_CHAR(fecha_ruta, 'YYYY-MM-DD') = '{fecha}' and lower(ciudad) = lower('{comuna}')   

               SELECT nombre_ruta,
                CASE WHEN bool_or(estado = FALSE) THEN FALSE ELSE TRUE END AS estado,
                CASE WHEN bool_or(pickeado = FALSE) THEN FALSE ELSE TRUE END AS pickeado,
                CASE WHEN bool_or(alerta = TRUE) THEN TRUE ELSE FALSE END AS alerta
            FROM quadminds.datos_ruta_manual
            WHERE TO_CHAR(fecha_ruta, 'YYYY-MM-DD') = '{fecha}' and lower(ciudad) = lower('{comuna}')
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
    
    ## editar rutas activas

    # def read_ruta_activa_by_nombre_ruta(self,nombre_ruta):
    #     with self.conn.cursor() as cur:
    #         cur.execute(f"""
    #         select id_ruta, nombre_ruta, cod_cliente, nombre, calle_numero, ciudad, provincia_estado, telefono, email, cod_pedido, fecha_pedido, cod_producto, desc_producto, cant_producto, notas, agrupador, sku, talla, estado, posicion, fecha_ruta
    #         from quadminds.datos_ruta_manual drm where nombre_ruta = '{nombre_ruta}'
    #         order by posicion
    #         """)
    #         return cur.fetchall()

    # def read_ruta_activa_by_nombre_ruta(self,nombre_ruta):
    #     with self.conn.cursor() as cur:
    #         cur.execute(f"""
    #         SELECT drm.id_ruta, drm.nombre_ruta, drm.cod_cliente, drm.nombre, drm.calle_numero, drm.ciudad, drm.provincia_estado, drm.telefono, drm.email, drm.cod_pedido, drm.fecha_pedido, drm.cod_producto, drm.desc_producto, drm.cant_producto, drm.notas, drm.agrupador, drm.sku, drm.talla, drm.estado, drm.posicion, drm.fecha_ruta, drm.de, drm.dp, tbm.alerta as "alerta TOC", tbm.observacion as "Obs. TOC" 
    #        -- ,(SELECT 
	# 		 --   CONCAT(
	# 		    --	CASE WHEN "Sistema" THEN '1' ELSE '0' END, '@',
	# 		      --  COALESCE("Obs. Sistema", 'null'), '@',
	# 		      --  CASE WHEN "Pistoleado" THEN '1' ELSE '0' END, '@',
	# 		       -- CASE WHEN "En Ruta" THEN '1' ELSE '0' END, '@',
    #                -- "Estado Entrega"
	# 		  --  ) AS concatenated_data
	# 		--FROM areati.busca_ruta_manual(drm.cod_pedido)
	# 		--LIMIT 1) as "Datos Extra"
    #         ,drm.alerta
    #         FROM quadminds.datos_ruta_manual drm 
    #         LEFT join
    #         (select * from rutas.toc_bitacora_mae tbm where tbm.alerta = true 
    #         ) as tbm ON tbm.guia = drm.cod_pedido 
    #         WHERE drm.nombre_ruta = '{nombre_ruta}' 
    #         ORDER BY drm.posicion;
    #         """)
    #         return cur.fetchall()

    def read_ruta_activa_by_nombre_ruta(self,nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select * from rutas.listar_ruta_edicion('{nombre_ruta}');
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
             easygo.recepcion as "Recepcion"   
        
            from areati.ti_carga_easy_go_opl easygo
            where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') 
            -- order by created_at desc  
            order by easygo.suborden      
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
    
    def read_recepcion_easy_cd(self):
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
            WHERE to_char(created_at,'yyyy-mm-dd hh24:mi')  >= to_char((obtener_dia_anterior() + INTERVAL '17 hours 30 minutes'),'yyyy-mm-dd hh24:mi')
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
             easygo.recepcion as "Recepcion"  
        
            from areati.ti_carga_easy_go_opl easygo
            --where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') AND easygo.suborden = '{codigo_pedido}'
            where easygo.suborden = '{codigo_pedido}'
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
            f"UPDATE areati.ti_wms_carga_sportex SET verified = true, recepcion = true  WHERE areati.ti_wms_carga_sportex.id_sportex = '{codigo_producto}'",
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
    

    def update_verified_sportex(self,codigo_pedido):
        with self.conn.cursor() as cur:
            cur.execute(f"""        
                UPDATE areati.ti_wms_carga_sportex 
                SET verified = true, recepcion = true 
                WHERE areati.ti_wms_carga_sportex.id_sportex = '{codigo_pedido}'
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
                WHERE easy.carton = '{codigo_producto}'
                """)
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
                WHERE easy.carton = '{codigo_producto}'
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
            
            cur.execute("""
                ---------------------------------------------------------------------------------------
                --  (1) Cuenta Easy CD
                ---------------------------------------------------------------------------------------
                select to_char(created_at,'yyyy-mm-dd') as "Fecha",
                to_char(created_at,'HH24:mi') as "Hora Ingreso", 
                'Easy CD' as "Cliente",
                easy.nro_carga as "N° Carga",
                (select count(distinct(entrega)) from areati.ti_wms_carga_easy e where e.nro_carga = easy.nro_carga) as "Entregas",
                (select count(*) from areati.ti_wms_carga_easy e where e.nro_carga = easy.nro_carga) as "Bultos",
                (select count(*) from areati.ti_wms_carga_easy e where e.nro_carga = easy.nro_carga and verified=true) as "Verificados",
                (select count(*) from areati.ti_wms_carga_easy e where e.nro_carga = easy.nro_carga and verified=false) as "Sin Verificar"
                from areati.ti_wms_carga_easy easy 
                WHERE to_char(created_at,'yyyy-mm-dd hh24:mi')  >= to_char((obtener_dia_anterior() + INTERVAL '17 hours 30 minutes'),'yyyy-mm-dd hh24:mi')
                AND to_char(created_at,'yyyy-mm-dd') <= to_char(CURRENT_DATE,'yyyy-mm-dd')
                group by 1,2,3,4
                ---------------------------------------------------------------------------------------
                --  (2) Cuenta Sportex
                ---------------------------------------------------------------------------------------
                union all
                select to_char(created_at,'yyyy-mm-dd') as "Fecha",
                to_char(created_at,'HH24:mi') as "Hora Ingreso", 
                'Sportex' as "Cliente",
                'Carga Unica' as "N° Carga",
                count(*) as "Entregas",
                count(*) as "Bultos",
                (select count(*) from areati.ti_wms_carga_sportex s where verified=true and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')) as "Verificados",
                (select count(*) from areati.ti_wms_carga_sportex s where verified=false and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')) as "Sin Verificar"
                from areati.ti_wms_carga_sportex twcs 
                where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
                group by 1,2,3,4
                ---------------------------------------------------------------------------------------
                --  (3) Cuenta Electrolux
                ---------------------------------------------------------------------------------------
                union all
                select to_char(created_at,'yyyy-mm-dd') as "Fecha",
                to_char(created_at,'HH24:mi') as "Hora Ingreso",  
                'Electrolux' as "Cliente",
                twce.ruta as "N° Carga",
                (select count(distinct(numero_guia)) 
                from areati.ti_wms_carga_electrolux e 
                where to_char(e.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') and twce.ruta=e.ruta) as "Entregas",
                count(*) as "Bultos",
                (select count(*) 
                from areati.ti_wms_carga_electrolux e
                where verified=true and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') and twce.ruta=e.ruta) as "Verificados",
                (select count(*) from areati.ti_wms_carga_electrolux e
                where verified=false and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') and twce.ruta=e.ruta) as "Sin Verificar"
                from areati.ti_wms_carga_electrolux twce 
                where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
                group by 1,2,3,4

                ---------------------------------------------------------------------------------------
                --  (4) Cuenta Easy OPL
                ---------------------------------------------------------------------------------------
                union all
                select to_char(opl.created_at,'yyyy-mm-dd') as "Fecha",
                to_char(opl.created_at,'HH24:mi') as "Hora Ingreso", 
                'Easy Tienda' as "Cliente",
                opl.id_ruta as "N° Carga",
                (select count(distinct(id_entrega)) from areati.ti_carga_easy_go_opl o where o.id_ruta = opl.id_ruta) as "Entregas",
                (select count(*) from areati.ti_carga_easy_go_opl o where o.id_ruta = opl.id_ruta) as "Bultos",
                (select count(*) from areati.ti_carga_easy_go_opl o where o.id_ruta = opl.id_ruta and verified=true) as "Verificados",
                (select count(*) from areati.ti_carga_easy_go_opl o where o.id_ruta = opl.id_ruta and verified=false) as "Sin Verificar"
                from areati.ti_carga_easy_go_opl opl 
                where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
                group by 1,2,3,4
                ---------------------------------------------------------------------------------------
                -- Orden por Hora de ingreso de productos al sistema
                ---------------------------------------------------------------------------------------
                order by 1 asc, 2 asc
                        """)
            
            return cur.fetchall()

    def read_datos_descarga_beetrack(self, id_ruta : str):
        with self.conn.cursor() as cur:
            cur.execute(f"""            
                select drm.cod_pedido as "NÚMERO GUÍA *",
                ra.patente as "VEHÍCULO *",
                split_part(drm.desc_producto, ') ', 2) as "NOMBRE ITEM *",
                drm.cant_producto as "CANTIDAD",
                drm.sku as "CODIGO ITEM",
                drm.cod_cliente as "IDENTIFICADOR CONTACTO *",
                drm.nombre as "NOMBRE CONTACTO",
                drm.telefono as "TELÉFONO",
                drm.email as "EMAIL CONTACTO",
                '' as "DIRECCIÓN *",
                '' as "LATITUD",
                '' as "LONGITUD",
                to_char(current_date,'yyyy-mm-dd') || ' 09:00' as "FECHA MIN ENTREGA",
                to_char(current_date,'yyyy-mm-dd') || ' 21:00' as "FECHA MAX ENTREGA",
                '' as "CT DESTINO",
                drm.calle_numero as "DIRECCION",
                '' as "DEPARTAMENTO",
                drm.ciudad as "COMUNA",
                drm.provincia_estado as "CIUDAD",
                'Chile' as "PAIS",
                drm.email as "EMAIL",
                drm.fecha_pedido as "Fechaentrega",
                (SELECT MAX(fecha_final) 
                FROM (
                SELECT TO_CHAR(MAX(created_at), 'yyyy-mm-dd') AS fecha_final
                FROM areati.ti_wms_carga_easy
                WHERE entrega = drm.cod_pedido
                
                UNION ALL
                
                SELECT TO_CHAR(MAX(created_at), 'yyyy-mm-dd') AS fecha_final
                FROM areati.ti_wms_carga_sportex
                WHERE id_sportex = drm.cod_pedido
                
                UNION ALL
                
                SELECT TO_CHAR(MAX(created_at), 'yyyy-mm-dd') AS fecha_final
                FROM areati.ti_wms_carga_electrolux
                WHERE numero_guia = drm.cod_pedido
                
                UNION ALL
                
                SELECT TO_CHAR(MAX(created_at), 'yyyy-mm-dd') AS fecha_final
                FROM areati.ti_carga_easy_go_opl
                WHERE suborden = drm.cod_pedido
                        
                UNION ALL

                SELECT TO_CHAR(MAX(created_at), 'yyyy-mm-dd') AS fecha_final
                FROM areati.ti_retiro_cliente trc 
                WHERE cod_pedido = drm.cod_pedido

                ) AS subconsulta) AS "fechahr",
                ra.driver as "conductor",
                split_part(initcap((regexp_matches(drm.desc_producto, '\((.*?)\)'))[1]), ' ', 1) as "Cliente",
                'Entrega a domicilio' as "Servicio",
                'CD ' || initcap((regexp_matches(drm.desc_producto, '\((.*?)\)'))[1])as "Origen",
                ra.region as "Región de despacho",
                drm.ciudad as "CMN",
                '' as "Peso",
                'S/I' as "Volumen",
                drm.cant_producto as "Bultos",
                '' as "ENTREGA",
                etlx.factura as "FACTURA",
                etlx.oc as "OC",
                etlx.ruta as "RUTA",
                etlx.tienda as "TIENDA"
                from quadminds.datos_ruta_manual drm
                left join hela.ruta_asignada ra on ra.id_ruta = drm.id_ruta
                left join areati.ti_wms_carga_electrolux etlx on etlx.numero_guia = drm.cod_pedido and etlx.codigo_item = drm.sku
                -- where drm.id_ruta = {id_ruta}
                where drm.nombre_ruta  = '{id_ruta}'
                order by posicion
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
        
    def obtener_alertas_vigentes(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select to_char(tbm.created_at,'yyyy-mm-dd') as "Fec. Creación", 
            tbm.guia as "Guía", 
            tbm.cliente as "Cliente", 
            coalesce(tbm.comuna_correcta || '*',tbm.comuna) as "Comuna", 
            coalesce(tbm.direccion_correcta || '*',  (select "Calle y Número" from areati.busca_ruta_manual(tbm.guia) limit 1)) as "Dirección",
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
             select * from rutas.listar_bitacora_fechas('{fecha_inicio}','{fecha_fin}'); 
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
            coalesce(tbm.direccion_correcta ,  (select "Calle y Número" from areati.busca_ruta_manual(tbm.guia) limit 1)) as "Dirección",
            coalesce(tbm.comuna_correcta ,tbm.comuna) as "Comuna", 
            tbm.subestado, tbm.subestado_esperado, tbm.observacion, tbm.codigo1 ,tbm.guia
            from  rutas.toc_bitacora_mae as tbm
            where ids_transyanez  = '{ids_ty}' limit 1
                        """)
            return cur.fetchall()
        

    def prueba_ty(self, offset):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select * from rutas.pendientes_prueba(null, null,{offset})
                        """)
            return cur.fetchall()
        
    def armar_rutas_bloque(self,data):
        with self.conn.cursor() as cur:
            cur.execute("""
                select * from rutas.armar_ruta_bloque(%(Codigos)s, %(Fecha_ruta)s, %(Id_user)s)
                        """,data)
            return cur.fetchall()
            

    ## New challenger  RSV

    def read_catalogo_rsv(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                select * from rsv.catalogo_productos
                        """)
            return cur.fetchall()

class transyanezConnection():
    conn = None
    def __init__(self) -> None:
        try:
            self.conn = psycopg2.connect(config("POSTGRES_DB_TR"))

        except psycopg2.OperationalError as err:
            print(err)
            self.conn.close()
            self.conn = psycopg2.connect(config("POSTGRES_DB_TR"))
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
        
