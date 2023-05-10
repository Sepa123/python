import psycopg2
import codecs
from decouple import config
import os, sys, codecs

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
            SELECT id,mail,"password" ,active ,rol_id  FROM "user".users WHERE mail=%(mail)s 
            """, data)
            return cur.fetchone()
        
class reportesConnection():
    conn = None
    def __init__(self) -> None:
        try:
            self.conn = psycopg2.connect(config("POSTGRES_DB_CARGA"))
            # self.conn.encoding("")
            self.conn.set_client_encoding("UTF-8")
        except psycopg2.OperationalError as err:
            print(err)
            self.conn.close()
        
    def __def__(self):
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
                Select count(*) from areati.ti_wms_carga_electrolux twce
                Where to_char(twce.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Electrolux",
            (
                Select count(*) from areati.ti_wms_carga_sportex twcs
                Where to_char(twcs.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Sportex",
            (
                Select count(*) from areati.ti_wms_carga_easy easy
                Where to_char(easy.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Easy",
            (
                Select count(*) from areati.ti_wms_carga_tiendas tienda
                Where to_char(tienda.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Tiendas"
            From generate_series(date(date_trunc('month', current_date)) - CURRENT_DATE, 0 ) i
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
                Select count(*) from areati.ti_wms_carga_electrolux twce
                Where to_char(twce.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Electrolux",
            (
                Select count(*) from areati.ti_wms_carga_sportex twcs
                Where to_char(twcs.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Sportex",
            (
                Select count(*) from areati.ti_wms_carga_easy easy
                Where to_char(easy.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Easy",
            (
                Select count(*) from areati.ti_wms_carga_tiendas tienda
                Where to_char(tienda.created_at,'yyyy-mm-dd') = to_char(CURRENT_DATE+ i,'yyyy-mm-dd')
            ) as "Tiendas"
            From generate_series(date'2023-01-01'- CURRENT_DATE, 0 ) i

            """)

            return cur.fetchall()
    #Cargas Verificadas y Total
    def read_cargas_easy(self):

        with self.conn.cursor() as cur:

            cur.execute("""
            Select 'Verificados: '||  count(*) as Cuenta from areati.ti_wms_carga_easy twce
            Where to_char(created_at,'yyyy-mm-dd')=to_char(current_date,'yyyy-mm-dd') and verified= true
            union
            Select 'Total: '|| count(*) as Cuenta from areati.ti_wms_carga_easy twce
            Where to_char(created_at,'yyyy-mm-dd')=to_char(current_date,'yyyy-mm-dd')
            """)
            return cur.fetchall()
    # Quadminds
    def read_clientes(self):

        with self.conn.cursor() as cur:

            cur.execute("""
            -------------------------------------------------------------------------------------------------------------------------------------
            -- EASY
            -------------------------------------------------------------------------------------------------------------------------------------
            select	CAST(easy.entrega AS varchar) AS "Código de Cliente",
                    initcap(easy.nombre) AS "Nombre",
                    CASE 
                        WHEN substring(easy.direccion from '^\d') ~ '\d' then substring(initcap(easy.direccion) from '\d+[\w\s]+\d+')
                        WHEN lower(easy.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
                        regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easy.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
                        -- cast(regexp_replace(regexp_replace(initcap(easy.direccion), ',.*$', ''), '\s+(\d+\D+\d+).*$', ' \1') AS varchar)
                        else substring(initcap(easy.direccion) from '^[^0-9]*[0-9]+')
                    END "Calle y Número",
                    
                    CASE
                        WHEN easy.region='XIII - Metropolitana' THEN 'Region Metropolitana'
                        WHEN easy.region='V - Valparaíso' THEN 'Valparaíso'
                        else (select initcap(tcr.region) from public.ti_comuna_region tcr where lower(tcr.comuna)=lower(easy.comuna))
                    END "Ciudad",
                    initcap(easy.comuna) AS "Provincia/Estado",
                    '' AS "Latitud",
                    '' AS "Longitud",
                    coalesce(easy.telefono,'0') AS "Teléfono con código de país",
                    lower(easy.Correo) AS "Email",
                    CAST(easy.entrega AS varchar) AS "Código de Pedido",
                    easy.fecha_entrega AS "Fecha de Pedido",
                    'E' AS "Operación E/R",
                    CAST(easy.carton AS varchar) AS "Código de Producto",
                    '(EASY) ' || easy.descripcion AS "Descripción del Producto",
                    easy.unid AS "Cantidad de Producto",
                    1 AS "Peso",
                    1 AS "Volumen",
                    1 AS "Dinero",
                    '8' AS "Duración min",
                    '09:00 - 21:00' AS "Ventana horaria 1",
                    '' AS "Ventana horaria 2",
                    coalesce((select 'Easy - ' || se.name from areati.subestado_entregas se where easy.subestado=se.code),'Easy') AS "Notas",
                    CASE
                        WHEN easy.region='XIII - Metropolitana' THEN 'RM'
                        WHEN easy.region='V - Valparaíso' THEN 'V - ' ||  initcap(easy.comuna)
                        else 'S/A'
                    END "Agrupador",
                    '' AS "Email de Remitentes",
                    '' AS "Eliminar Pedido Si - No - Vacío",
                    '' AS "Vehículo",
                    '' AS "Habilidades"
            from areati.ti_wms_carga_easy easy
            where lower(easy.nombre) not like '%easy%'
            and (easy.estado=0 or (easy.estado=2 and easy.subestado not in (7,10,12,43,50,51,70,80))) and easy.estado not in (1,3)
            and to_char(easy.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            
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
                        else substring(initcap(split_part(eltx.direccion,',',1)) from '^[^0-9]*[0-9]+')
                    END "Calle y Número",
                    initcap(split_part(eltx.direccion,',',3)) AS "Ciudad",
                    initcap(split_part(eltx.direccion,',',2))  AS "Provincia/Estado",
                    eltx.latitud AS "Latitud",
                    eltx.longitud AS "Longitud",
                    coalesce(eltx.telefono,'0') AS "Teléfono con código de país",
                    lower(eltx.email_contacto) AS "Email",
                    CAST (eltx.numero_guia AS varchar) AS "Código de Pedido",
                    eltx.fecha_min_entrega AS "Fecha de Pedido",
                    'E' AS "Operación E/R",
                    CAST (eltx.numero_guia AS varchar) AS "Código de Producto",
                    '(Electrolux) ' ||eltx.nombre_item AS "Descripción del Producto",
                    eltx.cantidad AS "Cantidad de Producto",
                    1 AS "Peso",
                    1 AS "Volumen",
                    1 AS "Dinero",
                    '8' AS "Duración min",
                    '09:00 - 21:00' AS "Ventana horaria 1",
                    '' AS "Ventana horaria 2",
                    coalesce((select 'Electrolux - ' || se.name from areati.subestado_entregas se where eltx.subestado=se.code),'Electrolux') AS "Notas",
                    'RM ' AS agrupador,
                    '' AS "Email de Remitentes",
                    '' AS "Eliminar Pedido Si - No - Vacío",
                    '' AS "Vehículo",
                    '' AS "Habilidades"
            from areati.ti_wms_carga_electrolux eltx
            where to_char(eltx.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
                and (eltx.estado=0 or (eltx.estado=2 and eltx.subestado not in (7,10,12,43,50,51,70,80))) and eltx.estado not in (1,3)

            -------------------------------------------------------------------------------------------------------------------------------------
            -- SPORTEX
            -------------------------------------------------------------------------------------------------------------------------------------
            union all
            select	twcs.id_sportex AS "Código de Cliente",
                    initcap(twcs.cliente) AS "Nombre",
                    CASE 
                        WHEN substring(initcap(replace(twcs.direccion,',','')) from '^\d') ~ '\d' then substring(initcap(replace(twcs.direccion,',','')) from '\d+[\w\s]+\d+')
                        WHEN lower(twcs.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN 
                        regexp_replace(regexp_replace(initcap(twcs.direccion), ',.*$', ''), '\s+(\d+\D+\d+).*$', ' \1')
                        else substring(initcap(replace(twcs.direccion,',','')) from '^[^0-9]*[0-9]+')
                    END "Calle y Número",
                    initcap(COALESCE(tcr.region, 'Region Metropolitana')) AS "Ciudad",
                    initcap(twcs.comuna)  AS "Provincia/Estado",
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
                    case
                        when tcr.region = 'Region Metropolitana' then 'RM'
                        when tcr.region = 'Valparaíso' then 'V - ' ||  initcap(twcs.comuna)
                    END "Agrupador",
                    '' AS "Email de Remitentes",
                    '' AS "Eliminar Pedido Si - No - Vacío",
                    '' AS "Vehículo",
                    '' AS "Habilidades"
            from areati.ti_wms_carga_sportex twcs
            left join ti_comuna_region tcr on
                translate(lower(twcs.comuna),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
            where to_char(twcs.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            and (twcs.estado=0 or (twcs.estado=2 and twcs.subestado not in (7,10,12,43,50,51,70,80))) and twcs.estado not in (1,3)

            -------------------------------------------------------------------------------------------------------------------------------------
            -- Easy OPL
            -------------------------------------------------------------------------------------------------------------------------------------
            union all  
            select	easygo.rut_cliente AS "Código de Cliente",
                    initcap(easygo.nombre_cliente) AS "Nombre",
                    initcap(easygo.direc_despacho) AS "Calle y Número",
                    initcap(COALESCE(tcr.region, 'Region Metropolitana')) AS "Ciudad",
                    initcap(easygo.comuna_despacho)  AS "Provincia/Estado",
                    '' AS "Latitud",
                    '' AS "Longitud",
                    coalesce(easygo.fono_cliente ,'0') AS "Teléfono con código de país",
                    lower(easygo.correo_cliente) AS "Email",
                    CAST (easygo.id_entrega AS varchar) AS "Código de Pedido",
                    easygo.fec_compromiso AS "Fecha de Pedido",
                    'E' AS "Operación E/R",
                    easygo.id_entrega AS "Código de Producto",
                    '(Easy OPL) ' || coalesce(easygo.tipo_pedido,'') AS "Descripción del Producto",
                    easygo.unidades  AS "Cantidad de Producto",
                    1 AS "Peso",
                    1 AS "Volumen",
                    1 AS "Dinero",
                    '8' AS "Duración min",
                    '09:00 - 21:00' AS "Ventana horaria 1",
                    '' AS "Ventana horaria 2",
                    coalesce((select 'Easy OPL - ' || se.name from areati.subestado_entregas se where easygo.subestado=se.code),'Easy OPL') AS "Notas",
                    case
                        when tcr.region = 'Region Metropolitana' then 'RM'
                        when tcr.region = 'Valparaíso' then 'V - ' ||  initcap(easygo.comuna_despacho)
                    END "Agrupador",
                    '' AS "Email de Remitentes",
                    '' AS "Eliminar Pedido Si - No - Vacío",
                    '' AS "Vehículo",
                    '' AS "Habilidades"
            from areati.ti_carga_easy_go_opl easygo
            left join ti_comuna_region tcr on
                translate(lower(easygo.comuna_despacho),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
            where to_char(easygo.created_at,'yyyymmdd')=to_char(current_date,'yyyymmdd')
            and (easygo.estado=0 or (easygo.estado=2 and easygo.subestado not in (7,10,12,43,50,51,70,80))) and easygo.estado not in (1,3)

            """)
            # nombre_tabla= tuple([desc[0] for desc in cur.description])
            
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

            nombre_tabla= tuple([desc[0] for desc in cur.description])

            return cur.fetchall()
    
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
        

