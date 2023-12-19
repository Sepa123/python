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
            SELECT id, full_name ,mail,"password" ,active ,rol_id  FROM "user".users WHERE lower(mail) = lower(%(mail)s)
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
            --select 
            --ROW_NUMBER() OVER (ORDER BY id_ruta) as "N°",
            --id_ruta as "Ruta",
            --patente as "Patente",
            --driver as "Driver", 
            --h_inc as "Inicio",
            --region as "Región",
            --t_ped as "Total Pedidos",
           -- h1100 || ' (' || p10 ||'%)' as "11:00 (10%)",
            --h1300 || ' (' || p40 ||'%)' as "13:00 (40%)",
            --h1500 || ' (' || p60 ||'%)' as "15:00 (60%)",
            --h1700 || ' (' || p80 ||'%)' as "17:00 (80%)",
            --h1800 || ' (' || p95 ||'%)' as "18:00 (95%)",
            --h2000 || ' (' || p100 ||'%)' as "20:00 (100%)",
            --t_ent as "Entregados",
            --n_ent as "No Entregados",
           -- p100 as "Porcentaje"
           -- from areati.mae_ns_ruta_beetrack_hoy 
                        
            select ROW_NUMBER() OVER (ORDER BY "ID. RUTA") as "N°", 
                "ID. RUTA",
                "PATENTE",
                "DRIVER",
                 TO_CHAR( "H_INIC", 'HH24:MI'),
                "REGION",
                "T-PED",
                "C11" || ' (' || "(%) 11" ||'%)' as "11:00 (10%)",
                "C13" || ' (' || "(%) 13" ||'%)' as "13:00 (40%)",
                "C15" || ' (' || "(%)15" ||'%)' as "15:00 (60%)",
                "C17" || ' (' || "(%)17" ||'%)' as "17:00 (80%)",
                "C18" || ' (' || "(%)18" ||'%)' as "18:00 (95%)",
                "C20" || ' (' || "(%)20" ||'%)' as "20:00 (100%)",
                "T-ENT",
                "N-ENT",
                "EE"
            from beetrack.recupera_ns_beetrack(to_char(current_date,'yyyymmdd'))
            where "FECHA" = current_date::date;

                        
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
            ---select * from areati.busca_ruta_manual('{pedido_id}')

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
        
    def get_factura_electrolux(self,pedido_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select codigo_item , folio_factura from areati.ti_wms_carga_electrolux eltx 
            where eltx.numero_guia = '{pedido_id}' or 
            trim(eltx.factura) = trim('{pedido_id}') or 
            trim(eltx.folio_factura) = trim('{pedido_id}') 
            """)
            return cur.fetchall()
        
    def get_numero_guia_by_factura(self,factura):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select numero_guia from areati.ti_wms_carga_electrolux eltx 
            where trim(eltx.factura) = trim('{factura}') 
            or trim(eltx.folio_factura) = trim('{factura}')
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
        
    def get_comuna_por_ruta_manual(self,pedido_id):

        with self.conn.cursor() as cur:
            cur.execute(f"""
            select "Ciudad" , "Provincia/Estado"  from areati.busca_ruta_manual('{pedido_id}') limit 1
            """)
            return cur.fetchone()

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
    
    def check_fecha_ruta_producto_existe(self,codigo_pedido):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            select fecha_ruta from quadminds.datos_ruta_manual drm 
            where cod_pedido = '{codigo_pedido}' or cod_producto = '{codigo_pedido}'
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
                (areati.busca_ruta_manual(subquery.cod_pedido))."Cod. SKU" as SKU,
                (areati.busca_ruta_manual(subquery.cod_pedido))."Cantidad de Producto" as bultos
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
            with data_ruta_manual as (
	SELECT 
		subquery.cod_pedido as cod_pedido,
		(areati.busca_ruta_manual(subquery.cod_pedido))."Cod. SKU" as sku ,
		cast((areati.busca_ruta_manual(subquery.cod_pedido))."Cantidad de Producto"  as varchar) as unidades
	FROM (
	  SELECT DISTINCT ON (drm.cod_pedido) drm.cod_pedido cod_pedido
	  FROM quadminds.datos_ruta_manual drm
	  WHERE drm.nombre_ruta = '{nombre_ruta}'
	) AS subquery
	group by 1 
)

--select * from areati.ti_carga_easy_go_opl tcego where suborden = '1274066301'
--select * from areati.busca_ruta_manual('1274066301')
--select * from data_ruta_manual

select ROW_NUMBER() over (ORDER BY id_ruta desc, posicion asc ) as "Pos.",* from
            (
            select drm.cod_pedido as "Cod. Pedido",
            drm.ciudad as "Comuna",
            string_agg(distinct(drm.sku) , '@') AS "SKU",
            string_agg(distinct(drm.desc_producto) , '@') AS "Producto",
            (SELECT 
			    --cod_pedido, 
			    STRING_AGG(unidades, '@') AS unidades_concatenados
			FROM 
			    data_ruta_manual
			where cod_pedido = drm.cod_pedido)	as "UND",
            --count(r.unidades) as "Bultos",
			--count(distinct(drm.sku)) as "Bultos",
            (SELECT  count(sku) AS bultos
			FROM data_ruta_manual
			where cod_pedido = drm.cod_pedido)	as "Bultos",
            initcap(nombre) as "Nombre Cliente",
            initcap(calle_numero) as "Direccion Cliente",
            drm.telefono as "Telefono",
            drm.estado as "Estado",
            '' as "Validado",
            CASE
                WHEN bool_or(de) THEN 'Embalaje con Daño'
                ELSE ''
            END AS "DE",
            CASE
                WHEN bool_or(dp) THEN 'Producto con Daño'
                ELSE ''
            END as "DP",
            drm.provincia_estado,
            drm.fecha_pedido,
            drm.id_ruta,
            drm.posicion
            from quadminds.datos_ruta_manual drm
            LEFT join data_ruta_manual r on r.cod_pedido = drm.cod_pedido 
            where drm.nombre_ruta = '{nombre_ruta}'
            group by 1,2,7,8,9,10,11, drm.id_ruta, drm.posicion, drm.provincia_estado, drm.fecha_pedido 
            ) datos_base;
            """)
            return cur.fetchall()
        

    def read_rutas_en_activo_para_armar_excel(self,nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
           select * from
            (select 	 distinct on (drm.cod_pedido) 
			drm.posicion as "N°",
            drm.cod_pedido as "Pedido",
            drm.ciudad as "Comuna",
            drm.nombre as "Nombre",
            drm.calle_numero "Dirección",
            drm.telefono as "teléfono",
            rsb.sku as "SKU",
            rsb.descripcion as "Producto",
            rsb.cant_producto as "UND",
            rsb.bultos as "Bult",
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
            LEFT JOIN LATERAL areati.recupera_sku_bultos(drm.cod_pedido) AS rsb ON true
            where drm.nombre_ruta =  '{nombre_ruta}'
            group by 1,2,3,4,5,6,7,8,9,10,13
            order by drm.cod_pedido ) as tabla 
            
            order by  tabla."N°" asc
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
                    r.fecha_original
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
                subquery.id_entrega,
                subquery.nombre_cliente,
                subquery.comuna_despacho,
                subquery.direc_despacho,
                subquery.total_unidades,
                subquery.verificado,
                coalesce(subquery.bultos, 1)
                
                from (
                    select 	distinct on (tcego.id_entrega)
                            tcego.id_ruta,
                            tcego.id_entrega,
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
            # ### original oscar
            # cur.execute("""
            #     ---------------------------------------------------------------------------------------
            #     --  (1) Cuenta Easy CD
            #     ---------------------------------------------------------------------------------------
            #     select to_char(created_at,'yyyy-mm-dd') as "Fecha",
            #     to_char(created_at,'HH24:mi') as "Hora Ingreso", 
            #     'Easy CD' as "Cliente",
            #     easy.nro_carga as "N° Carga",
            #     (select count(distinct(entrega)) from areati.ti_wms_carga_easy e where e.nro_carga = easy.nro_carga) as "Entregas",
            #     (select count(*) from areati.ti_wms_carga_easy e where e.nro_carga = easy.nro_carga) as "Bultos",
            #     (select count(*) from areati.ti_wms_carga_easy e where e.nro_carga = easy.nro_carga and verified=true) as "Verificados",
            #     (select count(*) from areati.ti_wms_carga_easy e where e.nro_carga = easy.nro_carga and verified=false) as "Sin Verificar"
            #     from areati.ti_wms_carga_easy easy 
            #     WHERE to_char(created_at,'yyyy-mm-dd hh24:mi')  >= to_char((obtener_dia_anterior() + INTERVAL '17 hours 30 minutes'),'yyyy-mm-dd hh24:mi')
            #     AND to_char(created_at,'yyyy-mm-dd') <= to_char(CURRENT_DATE,'yyyy-mm-dd')
            #     group by 1,2,3,4
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
            #     (select count(*) from areati.ti_wms_carga_sportex s where verified=true and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')) as "Verificados",
            #     (select count(*) from areati.ti_wms_carga_sportex s where verified=false and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')) as "Sin Verificar"
            #     from areati.ti_wms_carga_sportex twcs 
            #     where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            #     group by 1,2,3,4
            #     ---------------------------------------------------------------------------------------
            #     --  (3) Cuenta Electrolux
            #     ---------------------------------------------------------------------------------------
            #     union all
            #     select to_char(created_at,'yyyy-mm-dd') as "Fecha",
            #     to_char(created_at,'HH24:mi') as "Hora Ingreso",  
            #     'Electrolux' as "Cliente",
            #     twce.ruta as "N° Carga",
            #     (select count(distinct(numero_guia)) 
            #     from areati.ti_wms_carga_electrolux e 
            #     where to_char(e.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') and twce.ruta=e.ruta) as "Entregas",
            #     count(*) as "Bultos",
            #     (select count(*) 
            #     from areati.ti_wms_carga_electrolux e
            #     where verified=true and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') and twce.ruta=e.ruta) as "Verificados",
            #     (select count(*) from areati.ti_wms_carga_electrolux e
            #     where verified=false and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd') and twce.ruta=e.ruta) as "Sin Verificar"
            #     from areati.ti_wms_carga_electrolux twce 
            #     where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            #     group by 1,2,3,4

            #     ---------------------------------------------------------------------------------------
            #     --  (4) Cuenta Easy OPL
            #     ---------------------------------------------------------------------------------------
            #     union all
            #     select to_char(opl.created_at,'yyyy-mm-dd') as "Fecha",
            #     to_char(opl.created_at,'HH24:mi') as "Hora Ingreso", 
            #     'Easy Tienda' as "Cliente",
            #     opl.id_ruta as "N° Carga",
            #     (select count(distinct(id_entrega)) from areati.ti_carga_easy_go_opl o where o.id_ruta = opl.id_ruta) as "Entregas",
            #     (select count(*) from areati.ti_carga_easy_go_opl o where o.id_ruta = opl.id_ruta) as "Bultos",
            #     (select count(*) from areati.ti_carga_easy_go_opl o where o.id_ruta = opl.id_ruta and verified=true) as "Verificados",
            #     (select count(*) from areati.ti_carga_easy_go_opl o where o.id_ruta = opl.id_ruta and verified=false) as "Sin Verificar"
            #     from areati.ti_carga_easy_go_opl opl 
            #     where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            #     group by 1,2,3,4
            #     ---------------------------------------------------------------------------------------
            #     -- Orden por Hora de ingreso de productos al sistema
            #     ---------------------------------------------------------------------------------------
            #     order by 1 asc, 2 asc
            #             """)
            
            ### original mio

            cur.execute("""
                with aux_opl as(
                select
                id_ruta,
                COUNT(DISTINCT o.id_entrega) AS "Entregas",
                COUNT(*) AS "Bultos",
                COUNT(*) FILTER (WHERE o.verified = true) AS "Verificados",
                COUNT(*) FILTER (WHERE o.verified = false) AS "Sin Verificar"
                FROM
                areati.ti_carga_easy_go_opl o
                group by 1
                ),
                aux_elux as (
                    select e.ruta,
                        count(distinct(e.numero_guia)) AS "Entregas",
                        COUNT(*) AS "Bultos",
                        COUNT(*) FILTER (WHERE e.verified = true) AS "Verificados",
                        COUNT(*) FILTER (WHERE e.verified = false) AS "Sin Verificar"
                    from areati.ti_wms_carga_electrolux e 
                    group by 1
                ),
                aux_spo as (
                select
                        COUNT(*) FILTER (WHERE s.verified = true) AS "Verificados",
                        COUNT(*) FILTER (WHERE s.verified = false) AS "Sin Verificar"
                from areati.ti_wms_carga_sportex s
                where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
                ),
                aux_cd as (
                select
                    e.nro_carga,
                    COUNT(DISTINCT e.entrega) AS "Entregas",
                    COUNT(*) AS "Bultos",
                    COUNT(*) FILTER (WHERE e.verified = true) AS "Verificados",
                    COUNT(*) FILTER (WHERE e.verified = false) AS "Sin Verificar"
                FROM
                    areati.ti_wms_carga_easy e
                group by 1
                )
                
                ----------------
                --aqui empieza--
                ----------------
                
                ---------------------------------------------------------------------------------------
                --  (1) Cuenta Easy CD
                ---------------------------------------------------------------------------------------
                select to_char(created_at,'yyyy-mm-dd') as "Fecha",
                to_char(created_at,'HH24:mi') as "Hora Ingreso", 
                'Easy CD' as "Cliente",
                easy.nro_carga as "N° Carga",
                acd."Entregas",
                acd."Bultos",
                acd."Verificados",
                acd."Sin Verificar"
                from areati.ti_wms_carga_easy easy 
                left join aux_cd acd on easy.nro_carga = acd.nro_carga
                WHERE to_char(created_at,'yyyy-mm-dd hh24:mi')  >= to_char((obtener_dia_anterior() + INTERVAL '17 hours 30 minutes'),'yyyy-mm-dd hh24:mi')
                AND to_char(created_at,'yyyy-mm-dd') <= to_char(CURRENT_DATE,'yyyy-mm-dd')
                group by 1,2,3,4,5,6,7,8
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
                aspo."Verificados",
                aspo."Sin Verificar"
                --(select count(*) from areati.ti_wms_carga_sportex s where verified=true and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')) as "Verificados",
                --(select count(*) from areati.ti_wms_carga_sportex s where verified=false and to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')) as "Sin Verificar"
                from areati.ti_wms_carga_sportex twcs
                left join aux_spo aspo on true
                where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
                group by 1,2,3,4,7,8
                ---------------------------------------------------------------------------------------
                --  (3) Cuenta Electrolux
                ---------------------------------------------------------------------------------------
                union all
                select to_char(created_at,'yyyy-mm-dd') as "Fecha",
                to_char(created_at,'HH24:mi') as "Hora Ingreso",  
                'Electrolux' as "Cliente",
                twce.ruta as "N° Carga",
                aelux."Entregas",
                aelux."Bultos",
                aelux."Verificados",
                aelux."Sin Verificar"
                from areati.ti_wms_carga_electrolux twce 
                left join aux_elux aelux on twce.ruta = aelux.ruta
                where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
                group by 1,2,3,4,5,6,7,8

                ---------------------------------------------------------------------------------------
                --  (4) Cuenta Easy OPL
                ---------------------------------------------------------------------------------------
                union all
                select to_char(opl.created_at,'yyyy-mm-dd') as "Fecha",
                to_char(opl.created_at,'HH24:mi') as "Hora Ingreso", 
                'Easy Tienda' as "Cliente",
                opl.id_ruta as "N° Carga",
                aopl."Entregas",
                aopl."Bultos",
                aopl."Verificados",
                aopl."Sin Verificar"
                from areati.ti_carga_easy_go_opl opl 
                left join aux_opl aopl on opl.id_ruta = aopl.id_ruta
                where to_char(created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
                group by 1,2,3,4,5,6,7,8
                ---------------------------------------------------------------------------------------
                -- Orden por Hora de ingreso de productos al sistema
                ---------------------------------------------------------------------------------------
                order by 1 asc, 2 asc
                        """)
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
            coalesce(tbm.direccion_correcta ,  (select "Calle y Número" from areati.busca_ruta_manual(tbm.guia) limit 1)) as "Dirección",
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
                            coalesce(to_char(trb.fecha_llegada,' dd/mm/yyyy'),to_char(trb.created_at,' dd/mm/yyyy')) as dtOcorrencia,
                            coalesce(trb.fecha_llegada::time,created_at::time) as hrOcorrencia,
                            coalesce(trb.estado,'En Ruta') as comentario
                    from beetrack.ruta_transyanez trb
                    where created_at::date = current_date::date
                    and lower(cliente)='electrolux'
                    order by 4 desc
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
                       -- SELECT  *
                        -- from areati.registro_funciones;
                        SELECT rf.id, fecha_creacion, esquema, nombre_funcion, tf.nombre , descripcion,
                        coalesce(parametros, ARRAY['Sin parametros'] ), coalesce(comentarios_parametros, ARRAY['Sin comentario'] ), 
                        coalesce(palabras_clave,  ARRAY['Sin datos'] ), coalesce(tablas_impactadas, ARRAY['Sin datos'] )
                        FROM areati.registro_funciones rf
                        inner join areati.tipo_funciones tf  on rf.tipo_funcion = tf.id 
                        order by esquema, tf.nombre ;
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

    def ingresar_tipo_equipo(self, data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.tipo (nombre)
                        VALUES(%(nombre)s)
            """, data)
        self.conn.commit()

    def agregar_descripcion_equipo(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" INSERT INTO inventario.equipo (id_user, ids_user, lat, long, marca, modelo, serial, mac_wifi, serie, resolucion,
                         dimensiones, descripcion, ubicacion, almacenamiento, ram, estado, tipo, cantidad, nr_equipo)
                        VALUES(%(id_user)s,%(ids_user)s,%(lat)s,%(long)s,%(marca)s,%(modelo)s,%(serial)s,%(mac_wifi)s,%(serie)s,%(resolucion)s,
                        %(dimensiones)s,%(descripcion)s,%(ubicacion)s,%(almacenamiento)s,%(ram)s,%(estado)s,%(tipo)s)
                        """,data)
        self.conn.commit()

    def read_tipo_equipo(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT id, nombre FROM inventario.tipo; """)
            return cur.fetchall()
        
    def read_descripcion_equipo(self):
        with self.conn.cursor() as cur:
            cur.execute(""" SELECT e.id, e.marca, e.modelo, e.serial, e.mac_wifi, e.serie, e.resolucion, e.dimensiones, e.descripcion, e.ubicacion,
                        e.almacenamiento, e.ram, es.nombre AS estado, t.nombre AS tipo e.cantidad, e.nr_equipo 
                        FROM inventario.equipo e
                        INNER JOIN inventario.tipo t ON e.tipo = t.id
                        INNER JOIN inventario.estado es ON e.estado = es.id;""")
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
        
     ## devolucion de equipo    

    def asignar_devolucion_equipo(self, data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.asignacion  
                        SET estado=%(estado)s, fecha_devolucion=%(fecha_devolucion)s, observacion=%(observacion)s
                        WHERE folio=%(folio)s""", data)


        self.conn.commit()

    ##EDITAR TABLAS  DE INVENTARIO

    def editar_tipo_equipo(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.tipo
                        SET nombre=%(nombre)s where id=%(id)s""", data)
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

    def editar_descripcion_equipo(self,data):
        with self.conn.cursor() as cur:
            cur.execute(""" UPDATE inventario.equipo SET marca=%(marca)s,modelo=%(modelo)s,serial=%(serial)s,mac_wifi=%(mac_wifi)s,
	        serie=%(serie)s,resolucion=%(resolucion)s,dimensiones=%(dimensiones)s,descripcion=%(descripcion)s,ubicacion=%(ubicacion)s,
	        almacenamiento=%(almacenamiento)s,ram=%(ram)s,estado=%(estado)s,tipo=%(tipo)s where id=%(id)s """,data)
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
	        zapato=%(zapato)s""" , data)
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

    def ns_picking(self, fecha):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select * from areati.calcular_nivel_servicio_verificado('{fecha}')
                union all
                select 'Total',
                        sum(total_registros),
                        sum(productos_verificados),
                        case
                            when  sum(productos_verificados) > 0 then       
                            ROUND((sum(productos_verificados) / sum(total_registros) * 100), 2)
                    else 0
                    end
                from areati.calcular_nivel_servicio_verificado('{fecha}');
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
                JOIN LATERAL areati.busca_ruta_manual(subquery.guia) AS funcion_resultado ON true
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
                --JOIN LATERAL areati.busca_ruta_manual(subquery.guia) AS funcion_resultado ON true
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
                JOIN LATERAL areati.busca_ruta_manual(subquery.guia) AS funcion_resultado ON true
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
                JOIN LATERAL areati.busca_ruta_manual(subquery.guia) AS funcion_resultado ON true
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
                --JOIN LATERAL areati.busca_ruta_manual(subquery.guia) AS funcion_resultado ON true
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
                JOIN LATERAL areati.busca_ruta_manual(subquery.guia) AS funcion_resultado ON true
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
            -- JOIN LATERAL areati.busca_ruta_manual(subquery.guia) AS funcion_resultado ON true
                left join areati.estado_entregas ee on subquery.estado = ee.estado 
                left join areati.subestado_entregas se on subquery.subestado = se.code 
                where to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')>='{fecha_inicio}'
                and to_char(funcion_resultado."Fecha de Pedido",'yyyymmdd')<='{fecha_fin}'

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
        
