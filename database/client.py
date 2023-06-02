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
            self.conn.close()
        
    def __def__(self):
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
        
    def read_only_one(self, data):
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
    and (easy.estado=0 or (easy.estado=2 and easy.subestado not in (7,10,12,19,43,50,51,70,80))) and easy.estado not in (1,3)
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
                    and (eltx.estado=0 or (eltx.estado=2 and eltx.subestado not in (7,10,12,19,43,50,51,70,80))) and eltx.estado not in (1,3)
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
                and (twcs.estado=0 or (twcs.estado=2 and twcs.subestado not in (7,10,12,19,43,50,51,70,80))) and twcs.estado not in (1,3)

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
                and (easygo.estado=0 or (easygo.estado=2 and easygo.subestado not in (7,10,12,19,43,50,51,70,80))) and easygo.estado not in (1,3)
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
    and (easy.estado=0 or (easy.estado=2 and easy.subestado not in (7,10,12,19,43,50,51,70,80))) and easy.estado not in (1,3)
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
                    and (eltx.estado=0 or (eltx.estado=2 and eltx.subestado not in (7,10,12,19,43,50,51,70,80))) and eltx.estado not in (1,3)
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
                and (twcs.estado=0 or (twcs.estado=2 and twcs.subestado not in (7,10,12,19,43,50,51,70,80))) and twcs.estado not in (1,3)

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
                and (easygo.estado=0 or (easygo.estado=2 and easygo.subestado not in (7,10,12,19,43,50,51,70,80))) and easygo.estado not in (1,3)
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
                and (easy.estado=0 or (easy.estado=2 and easy.subestado not in (7,10,12,19,43,50,51,70,80))) and easy.estado not in (1,3)
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
                where (eltx.estado=0 or (eltx.estado=2 and eltx.subestado not in (7,10,12,19,43,50,51,70,80))) and eltx.estado not in (1,3)
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
                where (twcs.estado=0 or (twcs.estado=2 and twcs.subestado not in (7,10,12,19,43,50,51,70,80))) and twcs.estado not in (1,3)
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
                where (easygo.estado=0 or (easygo.estado=2 and easygo.subestado not in (7,10,12,19,43,50,51,70,80))) and easygo.estado not in (1,3)
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
            COUNT(CASE WHEN subquery.region = 'RM' THEN 1 END) AS "R. Metropolitana",
            COUNT(CASE WHEN subquery.region = 'V' THEN 1 END) AS "V Región"
            from (
            select distinct(easy.suborden) as entrega, easy.comuna_despacho as comuna,
            CASE
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna_despacho))) = 'Region Metropolitana' THEN 'RM'
                WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna_despacho))) = 'Valparaíso' THEN 'V'
                else 'N/A'
            END region
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
            easy.bultos as "Bultos"
            --,easy.estado,(select se."name"  from areati.subestado_entregas se where se.code = easy.subestado)
            from areati.ti_wms_carga_easy easy 
            where to_char(easy.fecha_entrega,'yyyymmdd') <= to_char(current_date,'yyyymmdd')
            and (easy.estado=0 or (easy.estado=2 and easy.subestado not in (7,10,12,19,43,50,51,70,80)))
            and easy.entrega not in (select guia from quadminds.ti_respuesta_beetrack)
            group by 1,2,3,4,5,6,7,8
            union all
            ------------------------------------------------------------------------------------------------------------------------------
            select 'Electrolux' as "Origen",
            numero_guia as "Cod. Entrega",
            to_char(created_at,'yyyy-mm-dd') as "Fecha Ingreso",
            to_char(fecha_max_entrega,'yyyy-mm-dd')  as "Fecha Compromiso",
            initcap(split_part(direccion,',',3)) AS "Region",
            initcap(split_part(direccion,',',2))  AS "Comuna",
            nombre_item as "Descripcion",
            cantidad as "Bultos"
            from areati.ti_wms_carga_electrolux twce 
            where to_char(twce.fecha_min_entrega,'yyyymmdd') <= to_char(current_date,'yyyymmdd')
            and (twce.estado=0 or (twce.estado=2 and twce.subestado not in (7,10,12,19,43,50,51,70,80)))
            and twce.numero_guia not in (select guia from quadminds.ti_respuesta_beetrack)
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
            1 as "Bultos"
            from areati.ti_wms_carga_sportex twcs  
            where to_char(twcs.fecha_entrega ,'yyyymmdd') <= to_char(current_date,'yyyymmdd')
            and (twcs.estado=0 or (twcs.estado=2 and twcs.subestado not in (7,10,12,19,43,50,51,70,80)))
            and twcs.id_sportex not in (select guia from quadminds.ti_respuesta_beetrack)
   
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
            easy.unidades as "Bultos"
            --select *
            from areati.ti_carga_easy_go_opl easy 
            where to_char(easy.fec_compromiso,'yyyymmdd') <= to_char(current_date,'yyyymmdd')
            and (easy.estado=0 or (easy.estado=2 and easy.subestado not in (7,10,12,19,43,50,51,70,80)))
            and easy.suborden not in (select guia from quadminds.ti_respuesta_beetrack)

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
            select * from areati.buscar_producto_picking('{producto_id}');
            """)
            return cur.fetchone()
        

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
        
    

class transyanezConnection():
    conn = None
    def __init__(self) -> None:
        try:
            self.conn = psycopg2.connect(config("POSTGRES_DB_TR"))

        except psycopg2.OperationalError as err:
            print(err)
            self.conn.close()
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
        

