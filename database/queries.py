
def recuperar_query(codigo_producto,):

    query_productos_4_digitos = f"""
    
with ruta_aux as (
select  distinct on (easy.carton)        
		-- CAST(easy.entrega AS varchar) AS "Código de Cliente",   
		null AS "Código de Cliente",  
        initcap(easy.nombre) AS "Nombre",
        coalesce(
        tbm.direccion,
        CASE 
	        WHEN substring(easy.direccion from '^\d') ~ '\d' then substring(initcap(easy.direccion) from '\d+[\w\s]+\d+')
	        WHEN lower(easy.direccion) ~ '^(pasaje|calle|avenida)\s+\d+\s+' THEN
	        regexp_replace(REPLACE(regexp_replace(regexp_replace(initcap(split_part(easy.direccion,',',1)), ',.$', ''), '\s+(\d+\D+\d+).$', ' \1'), '\', ''), '', '') 
	        else coalesce(substring(initcap(easy.direccion) from '^[^0-9]*[0-9]+'),initcap(easy.direccion))
        end) as "Calle y Número",
        coalesce(tbm.direccion,easy.direccion) as "Dirección Textual",
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
             -- lower(easy.Correo) AS "Email", -- [10/08/2023] Evitar que llegue correo a Cliente cuando
             --                                   el producto llega a la tienda.
        CASE
			WHEN LOWER(easy.nombre) LIKE '%easy%' THEN ''
			ELSE lower(easy.correo)
		END as "Email",
        CAST(easy.entrega AS varchar) AS "Código de Pedido",   -- Agrupar Por
        coalesce(tbm.fecha,
        CASE
        	WHEN twcep.fecha_entrega <> easy.fecha_entrega THEN twcep.fecha_entrega			-- Alertado por el Sistema
        	ELSE easy.fecha_entrega
        END
        ) AS "Fecha de Pedido",
        CASE
        	WHEN twcep.fecha_entrega <> easy.fecha_entrega THEN twcep.fecha_entrega			-- Alertado por el Sistema
        	ELSE easy.fecha_entrega
        end as "Fecha Original Pedido",														-- [H-001]
             'E' AS "Operación E/R",
             easy.carton AS "Código de Producto",
             '(EASY) ' || REPLACE(easy.descripcion, ',', '') AS "Descripción del Producto",
              CASE 
                WHEN easy.cant ~ '^\d+$' THEN (select count(*) 
                								from areati.ti_wms_carga_easy easy_a 
                								where easy_a.entrega = easy.entrega and easy_a.carton=easy.carton) 
                -- Si el campo es solo un número
                ELSE regexp_replace(easy.cant, '[^\d]', '', 'g')::numeric 
                -- Si el campo contiene una frase con cantidad
              END as "Cantidad de Producto",
            -- cast(easy.cant as numeric) AS "Cantidad de Producto", --15
             --select * from areati.ti_wms_carga_easy twce order by 2 desc
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
                    WHEN (select initcap(tcr.region) from public.ti_comuna_region tcr where unaccent(lower(tcr.comuna))=unaccent(lower(easy.comuna)))='Valparaíso' THEN 'V - ' ||  coalesce(tbm.comuna,initcap(easy.comuna))
                    else 'S/A'
             END "Agrupador",
             '' AS "Email de Remitentes",
             '' AS "Eliminar Pedido Si - No - Vacío",
             '' AS "Vehículo",
             '' AS "Habilidades",
             cast(easy.producto as text) as "Cod. SKU",                            -- no va a Quadminds
             CASE
	           WHEN easy.verified = TRUE OR easy.recepcion = TRUE THEN TRUE
	           ELSE FALSE
       		 END as "Pistoleado",
             -- easy.verified as "Pistoleado",                                     -- no va a Quadminds
             coalesce (tts.tamano,'?') as "Talla",                                 -- no va a Quadminds
             --initcap(ee.descripcion) as "Estado Entrega",           				-- no va a Quadminds
             case
             	when easy.estado=2 and easy.subestado in (7,10,12,19,43,44,50,51,70,80)
             	then 'NO SACAR A RUTA'
             	when easy.estado=3 
             	then 'NO SACAR A RUTA'
             	else initcap(ee.descripcion)
             end as "Estado Entrega",
             COALESCE(rb.identificador_ruta IS NOT NULL, false) as "En Ruta",       -- no va a Quadminds
             tbm.alerta as "TOC",                                                   -- Alertado por TOC
             tbm.observacion as "Observacion TOC",                                  -- Alertado por TOC
             CASE
        		WHEN (twcep.fecha_entrega <> easy.fecha_entrega) or (easy.estado=2 and easy.subestado in (7,10,12,19,43,44,50,51,70,80)) or (easy.estado=3)
        		THEN true			-- Alertado por el Sistema
        		ELSE false
    		 END AS "Sistema",
    		 case
             	when easy.estado=2 and easy.subestado in (7,10,12,19,43,44,50,51,70,80)
             	then (select name from areati.subestado_entregas where parent_code=2 and code=easy.subestado) || CHR(10)
             	when easy.estado=3 
             	then 'Archivado' || CHR(10)
             	else ''
             end
             ||
    		 CASE
        		WHEN twcep.fecha_entrega <> easy.fecha_entrega then      			-- Alertado por el Sistema
        		'Se Conserva la Fecha del Excel (' || twcep.fecha_entrega||') por diferencias con la API ('||easy.fecha_entrega||').'
        		ELSE ''
    		 END AS "Obs. Sistema"
from areati.ti_wms_carga_easy easy
left join public.ti_tamano_sku tts on tts.sku = cast(easy.producto as text)
left join areati.estado_entregas ee on ee.estado=easy.estado
--left join quadminds.ti_respuesta_beetrack rb on easy.entrega=rb.guia
left join beetrack.ruta_transyanez rb on (easy.entrega=rb.guia and rb.created_at::date = current_date)
left join ti_comuna_region tcr on
    translate(lower(easy.comuna),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
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
where SUBSTRING(easy.entrega FROM LENGTH(easy.entrega) - 3) = '{codigo_producto}'
  -- or SUBSTRING(easy.carton FROM LENGTH(easy.carton) - 3) = '{codigo_producto}'
-------------------------------------------------------------------------------------------------------------------------------------
-- ELECTROLUX
-- [22/09/2023] Se incorpora Alerta por sistema para productos que tienen condición de NO RETORNO A RUTA
-------------------------------------------------------------------------------------------------------------------------------------
union all
select eltx.identificador_contacto AS "Código de Cliente",
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
        coalesce(tbm.direccion,eltx.direccion) as "Dirección Textual",
        --initcap(split_part(eltx.direccion,',',2)) AS "Ciudad",
        --initcap(split_part(eltx.direccion,',',3)) AS "Provincia/Estado",
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
        eltx.latitud AS "Latitud",
        eltx.longitud AS "Longitud",
        coalesce(eltx.telefono,'0') AS "Teléfono con código de país",
        lower(eltx.email_contacto) AS "Email",
        CAST (eltx.numero_guia AS varchar) AS "Código de Pedido",
        coalesce(tbm.fecha,
        case
			--when calculo.fecha_siguiente <> eltx.fecha_min_entrega -- [21/09/2023] se corrige fecha siguiente con calendario Habil
	        when (select fecha_siguiente from areati.obtener_dias_habiles(to_char(eltx.created_at + interval '1 day','yyyymmdd'))) <> eltx.fecha_min_entrega
			then (select fecha_siguiente from areati.obtener_dias_habiles(to_char(eltx.created_at + interval '1 day','yyyymmdd')))
			else eltx.fecha_min_entrega
		end) AS "Fecha de Pedido",
		--eltx.fecha_min_entrega as "Fecha Original Pedido",							-- [H-001]
		case
	        when (select fecha_siguiente from areati.obtener_dias_habiles(to_char(eltx.created_at + interval '1 day','yyyymmdd'))) <> eltx.fecha_min_entrega
			then (select fecha_siguiente from areati.obtener_dias_habiles(to_char(eltx.created_at + interval '1 day','yyyymmdd')))
			else eltx.fecha_min_entrega
		end as "Fecha Original Pedido",
        'E' AS "Operación E/R",
        CAST (eltx.numero_guia AS varchar) AS "Código de Producto",
        '(Electrolux) ' || REPLACE(eltx.nombre_item, ',', '') AS "Descripción del Producto",
        cast(eltx.cantidad as numeric) AS "Cantidad de Producto",
        1 AS "Peso",
        1 AS "Volumen",
        1 AS "Dinero",
        '8' AS "Duración min",
        '09:00 - 21:00' AS "Ventana horaria 1",
        '' AS "Ventana horaria 2",
        coalesce((select 'Electrolux - ' || se.name from areati.subestado_entregas se where eltx.subestado=se.code and eltx.estado = se.parent_code),'Electrolux') AS "Notas",
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
       '' AS "Habilidades",
       cast(eltx.codigo_item as text) as "Cod. SKU",					-- no va a Quadminds
       CASE
	           WHEN eltx.verified = TRUE OR eltx.recepcion = TRUE THEN TRUE
	           ELSE FALSE
       END as "Pistoleado",
       --eltx.verified as "Pistoleado",									-- no va a Quadminds
       coalesce (tts.tamano,'?') as "Talla",							-- no va a Quadminds
       --initcap(ee.descripcion) as "Estado Entrega",      				-- no va a Quadminds
       case
             	when eltx.estado=2 and eltx.subestado in (7,10,12,19,43,44,50,51,70,80)
             	then 'NO SACAR A RUTA'
             	when eltx.estado=3 
             	then 'NO SACAR A RUTA'
             	else initcap(ee.descripcion)
             end as "Estado Entrega",
       COALESCE(rb.identificador_ruta IS NOT NULL, false) as "En Ruta",	-- no va a Quadminds
       tbm.alerta as "TOC",                                             -- Alertado por TOC
       tbm.observacion as "Observacion TOC",                            -- Alertado por TOC
       case
			when ((select fecha_siguiente from areati.obtener_dias_habiles(to_char(eltx.created_at + interval '1 day','yyyymmdd'))) <> eltx.fecha_min_entrega) 
			  or (eltx.estado=2 and eltx.subestado in (7,10,12,19,43,44,50,51,70,80)) or (eltx.estado=3)
			then true
			else false 
	   end AS "Sistema",                                              -- Alertado por el Sistema
       case
           	when eltx.estado=2 and eltx.subestado in (7,10,12,19,43,44,50,51,70,80)
           	then (select name from areati.subestado_entregas where parent_code=2 and code=eltx.subestado) || CHR(10)
           	when eltx.estado=3 
           	then 'Archivado' || CHR(10)
           	else ''
       end
       ||
	   case
			when (select fecha_siguiente from areati.obtener_dias_habiles(to_char(eltx.created_at + interval '1 day','yyyymmdd'))) <> eltx.fecha_min_entrega
			then 'Se calcula la Fecha real de entrega (' || (select fecha_siguiente from areati.obtener_dias_habiles(to_char(eltx.created_at + interval '1 day','yyyymmdd'))) ||') por diferencias con Excel ('||eltx.fecha_min_entrega||').'
			else '' 
	   end  AS "Obs. Sistema"                                             -- Alertado por el Sistema
from areati.ti_wms_carga_electrolux eltx
left join public.ti_tamano_sku tts on tts.sku = cast(eltx.codigo_item as text)
left join areati.estado_entregas ee on ee.estado=eltx.estado
--left join quadminds.ti_respuesta_beetrack rb on eltx.numero_guia=rb.guia
left join beetrack.ruta_transyanez rb on (eltx.numero_guia=rb.guia and rb.created_at::date = current_date)
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
where SUBSTRING(eltx.numero_guia FROM LENGTH(eltx.numero_guia) - 3) = '{codigo_producto}'
--or SUBSTRING(eltx.folio_factura FROM LENGTH(eltx.folio_factura) - 3) = '{codigo_producto}'

-------------------------------------------------------------------------------------------------------------------------------------
-- Easy OPL
-- [22/09/2023] Se incorpora Alerta por sistema para productos que tienen condición de NO RETORNO A RUTA
-------------------------------------------------------------------------------------------------------------------------------------
union all  
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
        easygo.id_entrega AS "Código de Producto",
        '(Easy OPL) ' || coalesce(REPLACE(easygo.descripcion, ',', ''),'') AS "Descripción del Producto",
        cast(easygo.unidades as numeric) AS "Cantidad de Producto",
        1 AS "Peso",
        1 AS "Volumen",
        1 AS "Dinero",
        '8' AS "Duración min",
        '09:00 - 21:00' AS "Ventana horaria 1",
        '' AS "Ventana horaria 2",
        --coalesce((select 'Easy OPL - ' || se.name from areati.subestado_entregas se where easygo.subestado=se.code),'Easy OPL') AS "Notas",
        'Easy OPL' AS "Notas",
        case
when tcr.region = 'Region Metropolitana' then 'RM - ' || coalesce (tts.tamano,'?')
when tcr.region = 'Valparaíso' then 'V - ' ||  initcap(easygo.comuna_despacho)
        END "Agrupador",
        '' AS "Email de Remitentes",
        '' AS "Eliminar Pedido Si - No - Vacío",
        '' AS "Vehículo",
        '' AS "Habilidades",
       cast(easygo.codigo_sku as text) as "Cod. SKU",					-- no va a Quadminds 
       CASE
	           WHEN easygo.verified = TRUE OR easygo.recepcion = TRUE THEN TRUE
	           ELSE FALSE
       END as "Pistoleado",
       --easygo.verified as "Pistoleado",							    -- no va a Quadminds
       coalesce (tts.tamano,'?') as "Talla",							-- no va a Quadminds
       --initcap(ee.descripcion) as "Estado Entrega",      				-- no va a Quadminds
       case
          	when easygo.estado=2 and easygo.subestado in (7,10,12,19,43,44,50,51,70,80)
          	then 'NO SACAR A RUTA'
          	when easygo.estado=3 
           	then 'NO SACAR A RUTA'
           	else initcap(ee.descripcion)
       end as "Estado Entrega",
       COALESCE(rb.identificador_ruta IS NOT NULL, false) as "En Ruta",	-- no va a Quadminds
       tbm.alerta as "TOC",                                             -- Alertado por TOC
       tbm.observacion as "Observacion TOC",                            -- Alertado por TOC
       case
			when (easygo.estado=2 and easygo.subestado in (7,10,12,19,43,44,50,51,70,80)) or (easygo.estado=3)
			then true
			else false 
	   end AS "Sistema",                                              -- Alertado por el Sistema
       case
           	when easygo.estado=2 and easygo.subestado in (7,10,12,19,43,44,50,51,70,80)
           	then (select name from areati.subestado_entregas where parent_code=2 and code=easygo.subestado) || CHR(10)
           	when easygo.estado=3 
           	then 'Archivado' || CHR(10)
           	else ''
       end AS "Obs. Sistema"                                             -- Alertado por el Sistema
from areati.ti_carga_easy_go_opl easygo
left join ti_comuna_region tcr on
    translate(lower(easygo.comuna_despacho),'áéíóúÁÉÍÓÚäëïöüÄËÏÖÜ','aeiouAEIOUaeiouAEIOU') = lower(tcr.comuna)
left join public.ti_tamano_sku tts on tts.sku = cast(easygo.codigo_sku as text)
left join areati.estado_entregas ee on ee.estado=easygo.estado
--left join quadminds.ti_respuesta_beetrack rb on easygo.suborden=rb.guia
left join beetrack.ruta_transyanez rb on (easygo.suborden=rb.guia and rb.created_at::date = current_date)
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

where SUBSTRING(easygo.suborden FROM LENGTH(easygo.suborden) - 3) = '{codigo_producto}' 
--or SUBSTRING(easygo.id_entrega FROM LENGTH(easygo.id_entrega) - 3) = '{codigo_producto}'

-------------------------------------------------------------------------------------------------------------------------------------
-- (5) Retiro Cliente [24/07/2023]
-- [22/09/2023] Se incorpora Alerta por sistema para productos que tienen condición de NO RETORNO A RUTA
-------------------------------------------------------------------------------------------------------------------------------------
union all
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
        retc.fecha_pedido as "Fecha Original Pedido",								-- [H-001]
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
where SUBSTRING(retc.cod_pedido FROM LENGTH(retc.cod_pedido) - 3) = '{codigo_producto}'


)


select distinct on (ruta."Código de Producto", "Cod. SKU" ) ruta."Código de Pedido", ruta."Código de Producto", ruta."Descripción del Producto", ruta."Ciudad",ruta."Provincia/Estado",ruta."Fecha de Pedido",ruta."Fecha Original Pedido"
,coalesce (drm.nombre_ruta , 'No asignada'), ruta."Notas", ruta."Cantidad de Producto"
from ruta_aux as ruta
left join quadminds.datos_ruta_manual drm on drm.cod_pedido = ruta."Código de Pedido" and drm.estado = true 



"""

    return query_productos_4_digitos