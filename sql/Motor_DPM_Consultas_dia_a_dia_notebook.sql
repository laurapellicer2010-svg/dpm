/* =============================================================================
   MOTOR DPM · CONSULTAS DEL DÍA A DÍA
   -----------------------------------------------------------------------------
   Dónde se ejecutan : celda %%sql de un notebook (Spark SQL) con el Lakehouse
                       DP_FET_LH_LegalAndRegulatoryReporting como predeterminado.
                       No sirven en el SQL analytics endpoint (T-SQL).
   Modelo de datos   : tablas dp_lgl_reg_dpm_* tal como las define
                       DE_DP_NB_LGL_REG_CREATE_TABLES (repo, 20/11/2026).
   Fechas            : todas las marcas de tiempo del motor están en UTC.

   CÓMO USARLAS
   Cada consulta empieza con un bloque  WITH params AS (...)  : basta con cambiar
   los valores de esa primera línea. Convención de parámetros:
       fw       framework en mayúsculas          'FCP', 'FINREP'...
       dp       ID Datapoint tal cual en Excel   'T02.00,i0240', 'F 01.01.r020.c010'
       periodo  AAAAMM  ('' = cualquiera)        '202512'
       exec_id  ''  = última ejecución terminada del framework (y periodo, si
                      se indica); las RUNNING se ignoran porque aún no han
                      escrito incidencias ni resultados
       vld_id   ''  = última validación del framework

   TRES DETALLES DEL MODELO QUE CONVIENE TENER PRESENTES
   1. Parametría vigente = ctrl_state = 1 AND row_present_flg = 1.
      Una fila con row_present_flg = 0 es una baja lógica (se quitó del Excel).
   2. En dp_lgl_reg_dpm_iss_log las incidencias de VALIDACIÓN llevan vld_id y
      exec_id vacío; las de EJECUCIÓN, al revés.
   3. En iss_log, var_id significa cosas distintas según la fase:
        validación -> código de variable del Excel (V1, V2...)
        ejecución  -> identificador interno (VAR_xxx / CALC_xxx); su código
                      V1, V2... se obtiene cruzando con lineage_var.

   ÍNDICE
   0. Panorama
      0.1 Estado de cada framework (carga, validación, ejecución, ¿listo?)
      0.2 Últimas ejecuciones de un framework
      0.3 Últimas validaciones de un framework
   1. Parametría vigente de un datapoint
      1.1 Ficha del datapoint (una fila por variable, con WHERE legible)
      1.2 Filtros en detalle
      1.3 Rollup: componentes y Rollups que usan el datapoint
      1.4 Historial de versiones del datapoint (qué cambió y cuándo)
   2. Errores de parametría (validación)
      2.1 Resumen de la última validación del framework
      2.2 Detalle de incidencias con la parametría implicada (filtrable por dp)
      2.3 Qué se ha corregido entre las dos últimas validaciones
      2.4 Historial de incidencias de validación de un datapoint
   3. Última ejecución del motor
      3.1 Resumen de la ejecución
      3.2 Detalle de incidencias (filtrable por dp)
      3.3 Pasos y tiempos de la ejecución
   4. Un datapoint en una ejecución
      4.1 Cabecera: fórmula, SQL del datapoint, estado e incidencias
      4.2 Variables: SELECT ejecutado, SELECT de detalle, importe e issue code
      4.3 Importes por vista y entidad, con el valor publicado
      4.4 Importe de cada variable por clave (entidad) y claves fallidas
   5. Otras utilidades
      5.1 Qué cambió en la última carga de un framework
      5.2 Datapoints del catálogo no publicados en un periodo y su motivo
      5.3 Comparar los importes de dos ejecuciones
      5.4 Qué datapoints usan una tabla, columna o valor de filtro
      5.5 Ejecuciones colgadas en RUNNING
      5.6 Significado de un código de incidencia
      5.7 Incidencias más frecuentes de los últimos 30 días
      5.8 Últimas subidas de parametría (DataLoader)
   Anexo. Re-ejecutar el linaje de un datapoint desde PySpark
   ============================================================================= */


/* =============================================================================
   0. PANORAMA
   ============================================================================= */

-- 0.1 Estado de cada framework ------------------------------------------------
-- Último cambio de parametría, última carga, último veredicto, última ejecución
-- y si se puede calcular: el gate de Processing exige un VALID posterior al último
-- cambio de la parametría del framework (filas nuevas o cerradas).
WITH params AS (SELECT '' AS fw),   -- '' = todos los frameworks
fw_files AS (
  SELECT DISTINCT UPPER(framework_cd) AS framework_cd, src_file_nm
  FROM dp_lgl_reg_dpm_param_datapoint
  WHERE ctrl_state = 1 AND row_present_flg = 1
),
param_all AS (
  SELECT src_file_nm, ref_start_dt, ref_end_dt, ref_last_dt, ctrl_state FROM dp_lgl_reg_dpm_param_datapoint
  UNION ALL SELECT src_file_nm, ref_start_dt, ref_end_dt, ref_last_dt, ctrl_state FROM dp_lgl_reg_dpm_param_fltr
  UNION ALL SELECT src_file_nm, ref_start_dt, ref_end_dt, ref_last_dt, ctrl_state FROM dp_lgl_reg_dpm_param_ctlg
  UNION ALL SELECT src_file_nm, ref_start_dt, ref_end_dt, ref_last_dt, ctrl_state FROM dp_lgl_reg_dpm_param_src_tbl
),
param_estado AS (
  SELECT f.framework_cd,
         concat_ws(', ', collect_set(f.src_file_nm))            AS ficheros,
         greatest(MAX(a.ref_start_dt), MAX(a.ref_end_dt))       AS ultimo_cambio_dt,
         MAX(CASE WHEN a.ctrl_state = 1 THEN a.ref_last_dt END) AS ultima_carga_dt
  FROM fw_files f
  JOIN param_all a ON a.src_file_nm = f.src_file_nm
  GROUP BY f.framework_cd
),
n_dp AS (
  SELECT UPPER(framework_cd) AS framework_cd,
         COUNT(DISTINCT datapoint_id) AS datapoints,
         COUNT(*)                     AS filas_datapoints
  FROM dp_lgl_reg_dpm_param_datapoint
  WHERE ctrl_state = 1 AND row_present_flg = 1
  GROUP BY UPPER(framework_cd)
),
vld AS (
  SELECT * FROM (
    SELECT UPPER(framework_cd) AS framework_cd, vld_id, param_snpsh_dt, vld_stts, err_nr, crtn_dt,
           ROW_NUMBER() OVER (PARTITION BY UPPER(framework_cd) ORDER BY crtn_dt DESC) AS rn
    FROM dp_lgl_reg_dpm_param_vld
  ) WHERE rn = 1
),
ult_valid AS (
  SELECT UPPER(framework_cd) AS framework_cd, MAX(param_snpsh_dt) AS ult_valid_snpsh_dt
  FROM dp_lgl_reg_dpm_param_vld
  WHERE vld_stts = 'VALID'
  GROUP BY UPPER(framework_cd)
),
ejec AS (
  SELECT * FROM (
    SELECT UPPER(TRIM(x.fw)) AS framework_cd, e.exec_id, e.ref_dt, e.exec_stts,
           e.warning_nr, e.err_nr, e.start_dt, e.end_dt,
           ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(x.fw)) ORDER BY e.start_dt DESC) AS rn
    FROM dp_lgl_reg_dpm_exec e
    LATERAL VIEW explode(split(e.framework_list_ln, ',')) x AS fw
  ) WHERE rn = 1
)
SELECT pe.framework_cd,
       pe.ficheros,
       n.datapoints,
       n.filas_datapoints,
       pe.ultimo_cambio_dt,
       pe.ultima_carga_dt,
       v.vld_stts            AS ultimo_veredicto,
       v.err_nr              AS errores_ultima_validacion,
       v.param_snpsh_dt      AS instantanea_validada,
       v.crtn_dt             AS validado_dt,
       CASE WHEN uv.ult_valid_snpsh_dt >= pe.ultimo_cambio_dt THEN 'SI'
            ELSE 'NO: validar la última carga' END AS listo_para_calcular,
       e.exec_id             AS ultima_ejecucion,
       date_format(e.ref_dt, 'yyyyMM') AS periodo_ultima_ejecucion,
       e.exec_stts,
       e.warning_nr,
       e.err_nr,
       e.start_dt,
       e.end_dt
FROM param_estado pe
CROSS JOIN params p
LEFT JOIN n_dp n       ON n.framework_cd  = pe.framework_cd
LEFT JOIN vld v        ON v.framework_cd  = pe.framework_cd
LEFT JOIN ult_valid uv ON uv.framework_cd = pe.framework_cd
LEFT JOIN ejec e       ON e.framework_cd  = pe.framework_cd
WHERE p.fw = '' OR pe.framework_cd = p.fw
ORDER BY pe.framework_cd;


-- 0.2 Últimas ejecuciones de un framework --------------------------------------
WITH params AS (SELECT 'FCP' AS fw, '' AS periodo)
SELECT e.exec_id,
       e.req_id,
       date_format(e.ref_dt, 'yyyyMM') AS periodo,
       e.framework_list_ln,
       e.exec_stts,
       e.warning_nr,
       e.err_nr,
       e.param_snpsh_dt,
       e.start_dt,
       e.end_dt,
       ROUND((unix_timestamp(e.end_dt) - unix_timestamp(e.start_dt)) / 60, 1) AS minutos,
       e.msg_ln
FROM dp_lgl_reg_dpm_exec e
CROSS JOIN params p
WHERE array_contains(split(UPPER(e.framework_list_ln), ','), p.fw)
  AND (p.periodo = '' OR date_format(e.ref_dt, 'yyyyMM') = p.periodo)
ORDER BY e.start_dt DESC
LIMIT 20;


-- 0.3 Últimas validaciones de un framework -------------------------------------
WITH params AS (SELECT 'FCP' AS fw)
SELECT v.vld_id, v.framework_cd, v.param_snpsh_dt, v.vld_stts, v.err_nr, v.crtn_dt
FROM dp_lgl_reg_dpm_param_vld v
CROSS JOIN params p
WHERE UPPER(v.framework_cd) = p.fw
ORDER BY v.crtn_dt DESC
LIMIT 20;


/* =============================================================================
   1. PARAMETRÍA VIGENTE DE UN DATAPOINT (última versión cargada)
   ============================================================================= */

-- 1.1 Ficha del datapoint -------------------------------------------------------
-- Una fila por variable: coordenadas EBA, tipo, tabla lógica y física, columna y
-- formato de periodo, clave de agrupación, métrica, HAVING, periodo y un WHERE
-- legible con los filtros. El WHERE es orientativo: el SQL exacto que ejecuta el
-- motor (tipos, TRIM/UPPER, <NULL>) está en lineage_var (consulta 4.2).
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp),
d AS (
  SELECT d.*
  FROM dp_lgl_reg_dpm_param_datapoint d
  JOIN params p ON UPPER(d.framework_cd) = p.fw AND TRIM(d.datapoint_id) = p.dp
  WHERE d.ctrl_state = 1 AND d.row_present_flg = 1
),
f AS (
  SELECT f.src_file_nm, f.datapoint_id, UPPER(COALESCE(f.var_cd, '')) AS var_cd,
         array_join(array_sort(collect_list(
           concat(f.src_column_nm, ' ', UPPER(f.optr_cd), ' ', f.fltr_val_ln))), '  AND  ') AS where_legible,
         COUNT(*) AS n_filtros
  FROM dp_lgl_reg_dpm_param_fltr f
  JOIN (SELECT DISTINCT src_file_nm, datapoint_id FROM d) dd
    ON dd.src_file_nm = f.src_file_nm AND dd.datapoint_id = f.datapoint_id
  WHERE f.ctrl_state = 1 AND f.row_present_flg = 1
  GROUP BY f.src_file_nm, f.datapoint_id, UPPER(COALESCE(f.var_cd, ''))
)
SELECT DISTINCT
       d.framework_cd,
       d.src_file_nm,
       d.datapoint_id,
       c.tmplt_cd, c.row_cd, c.column_cd,
       COALESCE(NULLIF(c.val_typ, ''), 'Amount (por defecto)') AS val_typ,
       d.datapoint_cl_typ,
       COALESCE(NULLIF(d.var_cd, ''), CASE WHEN d.src_tbl_id = '' THEN '(fórmula / expresión)' ELSE '(única)' END) AS variable,
       d.src_tbl_id,
       s.phy_tbl_nm,
       s.per_column_nm,
       s.per_fmt_cd,
       concat_ws(' + ', NULLIF(s.ent_column_nm, ''), NULLIF(s.ident_column_nm, ''),
                        NULLIF(s.subent_column_nm, ''), NULLIF(s.ctry_column_nm, '')) AS clave_agrupacion,
       COALESCE(NULLIF(s.grain_id, ''), UPPER(d.src_tbl_id))   AS grain_id,
       d.metric_fmla_ln,
       d.having_ln,
       COALESCE(NULLIF(d.per_expr_cd, ''), CASE WHEN d.src_tbl_id <> '' THEN 'M' END) AS periodo,
       f.n_filtros,
       f.where_legible,
       d.src_row_nr      AS fila_excel,
       d.ref_start_dt    AS version_desde,
       d.ref_last_dt     AS vista_en_ultima_carga
FROM d
LEFT JOIN dp_lgl_reg_dpm_param_ctlg c
       ON c.src_file_nm = d.src_file_nm AND c.datapoint_id = d.datapoint_id
      AND c.ctrl_state = 1 AND c.row_present_flg = 1
LEFT JOIN dp_lgl_reg_dpm_param_src_tbl s
       ON s.src_file_nm = d.src_file_nm AND UPPER(s.src_tbl_id) = UPPER(d.src_tbl_id)
      AND s.ctrl_state = 1 AND s.row_present_flg = 1
LEFT JOIN f
       ON f.src_file_nm = d.src_file_nm AND f.datapoint_id = d.datapoint_id
      AND f.var_cd = UPPER(COALESCE(d.var_cd, ''))
ORDER BY fila_excel;


-- 1.2 Filtros del datapoint en detalle -----------------------------------------
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp),
ficheros AS (
  SELECT DISTINCT src_file_nm
  FROM dp_lgl_reg_dpm_param_datapoint d
  JOIN params p ON UPPER(d.framework_cd) = p.fw
  WHERE d.ctrl_state = 1 AND d.row_present_flg = 1
)
SELECT f.datapoint_id,
       COALESCE(NULLIF(f.var_cd, ''), '(única)') AS variable,
       f.src_column_nm,
       f.optr_cd,
       f.fltr_val_ln,
       size(split(f.fltr_val_ln, ','))           AS n_valores_aprox,
       f.src_row_nr                              AS fila_excel,
       f.ref_start_dt                            AS version_desde
FROM dp_lgl_reg_dpm_param_fltr f
JOIN ficheros fi ON fi.src_file_nm = f.src_file_nm
CROSS JOIN params p
WHERE f.ctrl_state = 1 AND f.row_present_flg = 1
  AND TRIM(f.datapoint_id) = p.dp
ORDER BY variable, fila_excel;


-- 1.3a Rollup: componentes del datapoint ---------------------------------------
-- Desglosa la expresión [A] + [B] - [C] y comprueba cada componente en el mismo
-- fichero (tipo, tabla, periodo). Señala los componentes que no existen.
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0230' AS dp),
vig AS (
  SELECT * FROM dp_lgl_reg_dpm_param_datapoint
  WHERE ctrl_state = 1 AND row_present_flg = 1
),
r AS (
  SELECT v.src_file_nm, v.datapoint_id, v.metric_fmla_ln,
         regexp_extract_all(v.metric_fmla_ln, '([+-]?)\\s*\\[', 1)      AS signos,
         regexp_extract_all(v.metric_fmla_ln, '\\[([^\\]]+)\\]', 1)     AS ids
  FROM vig v
  JOIN params p ON UPPER(v.framework_cd) = p.fw AND TRIM(v.datapoint_id) = p.dp
  WHERE UPPER(v.datapoint_cl_typ) = 'ROLLUP'
),
comp AS (
  SELECT r.src_file_nm, r.datapoint_id AS rollup_id, r.metric_fmla_ln,
         CASE WHEN z.signos = '-' THEN -1 ELSE 1 END AS signo,
         TRIM(z.ids) AS componente
  FROM r
  LATERAL VIEW explode(arrays_zip(r.signos, r.ids)) t AS z
)
SELECT c.rollup_id,
       c.metric_fmla_ln,
       c.signo,
       c.componente,
       CASE WHEN v.datapoint_id IS NULL THEN 'NO EXISTE EN EL FICHERO' ELSE v.datapoint_cl_typ END AS tipo_componente,
       v.var_cd,
       v.src_tbl_id,
       COALESCE(NULLIF(v.per_expr_cd, ''), CASE WHEN v.src_tbl_id <> '' THEN 'M' END) AS periodo,
       v.metric_fmla_ln AS metrica_componente
FROM comp c
LEFT JOIN vig v ON v.src_file_nm = c.src_file_nm AND TRIM(v.datapoint_id) = c.componente
ORDER BY c.componente, v.var_cd;


-- 1.3b Rollups que usan el datapoint (impacto de cambiarlo) ---------------------
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp)
SELECT v.datapoint_id AS rollup_que_lo_usa,
       v.metric_fmla_ln,
       v.src_file_nm
FROM dp_lgl_reg_dpm_param_datapoint v
CROSS JOIN params p
WHERE v.ctrl_state = 1 AND v.row_present_flg = 1
  AND UPPER(v.framework_cd) = p.fw
  AND UPPER(v.datapoint_cl_typ) = 'ROLLUP'
  AND array_contains(transform(regexp_extract_all(v.metric_fmla_ln, '\\[([^\\]]+)\\]', 1), x -> TRIM(x)), p.dp)
ORDER BY rollup_que_lo_usa;


-- 1.4 Historial de versiones del datapoint --------------------------------------
-- Todas las versiones de sus filas en Datapoints, Filters y Catalog, con su
-- vigencia. Útil para saber qué cambió, cuándo y si hay bajas lógicas.
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp),
ficheros AS (
  SELECT DISTINCT src_file_nm
  FROM dp_lgl_reg_dpm_param_datapoint d
  JOIN params p ON UPPER(d.framework_cd) = p.fw
),
h AS (
  SELECT 'Datapoints' AS pestana, src_file_nm, src_row_nr, datapoint_id, var_cd,
         concat_ws(' | ', datapoint_cl_typ, src_tbl_id, metric_fmla_ln, having_ln, per_expr_cd) AS contenido,
         ref_start_dt, ref_end_dt, ctrl_state, row_present_flg
  FROM dp_lgl_reg_dpm_param_datapoint
  UNION ALL
  SELECT 'Filters', src_file_nm, src_row_nr, datapoint_id, var_cd,
         concat_ws(' | ', src_column_nm, optr_cd, fltr_val_ln),
         ref_start_dt, ref_end_dt, ctrl_state, row_present_flg
  FROM dp_lgl_reg_dpm_param_fltr
  UNION ALL
  SELECT 'Catalog', src_file_nm, src_row_nr, datapoint_id, CAST(NULL AS STRING),
         concat_ws(' | ', tmplt_cd, row_cd, column_cd, val_typ),
         ref_start_dt, ref_end_dt, ctrl_state, row_present_flg
  FROM dp_lgl_reg_dpm_param_ctlg
)
SELECT h.pestana,
       h.var_cd,
       h.contenido,
       CASE WHEN h.row_present_flg = 0 THEN 'BAJA LÓGICA'
            WHEN h.ctrl_state = 1     THEN 'VIGENTE'
            ELSE 'HISTÓRICA' END    AS estado,
       h.ref_start_dt           AS desde,
       h.ref_end_dt             AS hasta,
       h.src_row_nr             AS fila_excel,
       h.src_file_nm
FROM h
JOIN ficheros fi ON fi.src_file_nm = h.src_file_nm
CROSS JOIN params p
WHERE TRIM(h.datapoint_id) = p.dp
ORDER BY h.ref_start_dt DESC, h.pestana, h.var_cd;


/* =============================================================================
   2. ERRORES DE PARAMETRÍA (VALIDACIÓN)
   ============================================================================= */

-- 2.1 Resumen de la última validación del framework ------------------------------
WITH params AS (SELECT 'FCP' AS fw, '' AS vld_id),
ult AS (
  SELECT v.vld_id, UPPER(v.framework_cd) AS framework_cd, v.param_snpsh_dt,
         v.vld_stts, v.err_nr, v.crtn_dt
  FROM dp_lgl_reg_dpm_param_vld v
  CROSS JOIN params p
  WHERE UPPER(v.framework_cd) = p.fw AND (p.vld_id = '' OR v.vld_id = p.vld_id)
  ORDER BY v.crtn_dt DESC
  LIMIT 1
)
SELECT u.vld_id,
       u.crtn_dt                        AS validado_dt,
       u.param_snpsh_dt,
       u.vld_stts,
       u.err_nr,
       i.iss_cd,
       COUNT(i.iss_id)                  AS ocurrencias,
       COUNT(DISTINCT i.datapoint_id)   AS datapoints_afectados,
       MIN(i.msg_ln)                    AS ejemplo
FROM ult u
LEFT JOIN dp_lgl_reg_dpm_iss_log i
       ON i.vld_id = u.vld_id
      AND UPPER(COALESCE(i.framework_cd, '')) IN (u.framework_cd, '')
GROUP BY u.vld_id, u.crtn_dt, u.param_snpsh_dt, u.vld_stts, u.err_nr, i.iss_cd
ORDER BY ocurrencias DESC, i.iss_cd;


-- 2.2 Detalle de incidencias con la parametría implicada --------------------------
-- Cada incidencia de la validación junto a la fila del Excel que la provoca
-- (en la instantánea validada, no en la vigente). dp = '' para ver todas.
WITH params AS (SELECT 'FCP' AS fw, '' AS dp, '' AS vld_id),
ult AS (
  SELECT v.vld_id, UPPER(v.framework_cd) AS framework_cd, v.param_snpsh_dt
  FROM dp_lgl_reg_dpm_param_vld v
  CROSS JOIN params p
  WHERE UPPER(v.framework_cd) = p.fw AND (p.vld_id = '' OR v.vld_id = p.vld_id)
  ORDER BY v.crtn_dt DESC
  LIMIT 1
),
iss AS (
  SELECT i.*
  FROM dp_lgl_reg_dpm_iss_log i
  JOIN ult u ON i.vld_id = u.vld_id
  CROSS JOIN params p
  WHERE UPPER(COALESCE(i.framework_cd, '')) IN (u.framework_cd, '')
    AND (p.dp = '' OR TRIM(i.datapoint_id) = p.dp)
),
-- parametría de la instantánea validada
snap_dp AS (
  SELECT d.*
  FROM dp_lgl_reg_dpm_param_datapoint d
  JOIN ult u ON UPPER(d.framework_cd) = u.framework_cd
  WHERE d.ref_start_dt <= u.param_snpsh_dt
    AND (d.ref_end_dt IS NULL OR d.ref_end_dt > u.param_snpsh_dt)
    AND d.row_present_flg = 1
),
snap_f AS (
  SELECT f.src_file_nm, f.datapoint_id, UPPER(COALESCE(f.var_cd, '')) AS var_cd,
         array_join(array_sort(collect_list(
           concat(f.src_column_nm, ' ', UPPER(f.optr_cd), ' ', f.fltr_val_ln))), '  AND  ') AS where_legible
  FROM dp_lgl_reg_dpm_param_fltr f
  CROSS JOIN ult u
  WHERE f.ref_start_dt <= u.param_snpsh_dt
    AND (f.ref_end_dt IS NULL OR f.ref_end_dt > u.param_snpsh_dt)
    AND f.row_present_flg = 1
  GROUP BY f.src_file_nm, f.datapoint_id, UPPER(COALESCE(f.var_cd, ''))
)
SELECT i.iss_cd,
       i.datapoint_id,
       NULLIF(i.var_id, '')          AS variable,
       NULLIF(i.src_tbl_id, '')      AS tabla_incidencia,
       i.afct_nr,
       i.msg_ln,
       d.datapoint_cl_typ,
       d.src_tbl_id                  AS tabla_parametria,
       d.metric_fmla_ln,
       d.having_ln,
       d.per_expr_cd,
       f.where_legible,
       d.src_row_nr                  AS fila_excel_datapoints,
       d.src_file_nm
FROM iss i
LEFT JOIN snap_dp d
       ON TRIM(d.datapoint_id) = TRIM(i.datapoint_id)
      AND (COALESCE(i.var_id, '') = ''
           OR UPPER(d.var_cd) = UPPER(i.var_id)
           OR (COALESCE(d.var_cd, '') = '' AND UPPER(i.var_id) = 'V1'))
LEFT JOIN snap_f f
       ON f.src_file_nm = d.src_file_nm AND f.datapoint_id = d.datapoint_id
      AND f.var_cd = UPPER(COALESCE(d.var_cd, ''))
ORDER BY i.iss_cd, i.datapoint_id, variable;


-- 2.3 Qué se ha corregido entre las dos últimas validaciones ----------------------
-- CORREGIDA = estaba en la penúltima y ya no está; NUEVA = aparece en la última.
WITH params AS (SELECT 'FCP' AS fw),
v AS (
  SELECT vld_id, crtn_dt, ROW_NUMBER() OVER (ORDER BY crtn_dt DESC) AS rn
  FROM dp_lgl_reg_dpm_param_vld v
  JOIN params p ON UPPER(v.framework_cd) = p.fw
),
i AS (
  SELECT DISTINCT v.rn, i.iss_cd, i.datapoint_id, i.var_id, i.msg_ln
  FROM dp_lgl_reg_dpm_iss_log i
  JOIN v ON i.vld_id = v.vld_id AND v.rn IN (1, 2)
  CROSS JOIN params p
  WHERE UPPER(COALESCE(i.framework_cd, '')) IN (p.fw, '')
),
ultima    AS (SELECT * FROM i WHERE rn = 1),
penultima AS (SELECT * FROM i WHERE rn = 2)
SELECT CASE WHEN u.iss_cd IS NULL THEN 'CORREGIDA'
            WHEN pu.iss_cd IS NULL THEN 'NUEVA'
            ELSE 'SIGUE' END                       AS situacion,
       COALESCE(u.iss_cd, pu.iss_cd)               AS iss_cd,
       COALESCE(u.datapoint_id, pu.datapoint_id)   AS datapoint_id,
       COALESCE(u.var_id, pu.var_id)               AS variable,
       COALESCE(u.msg_ln, pu.msg_ln)               AS msg_ln
FROM ultima u
FULL OUTER JOIN penultima pu
  ON  u.iss_cd = pu.iss_cd
  AND u.datapoint_id <=> pu.datapoint_id
  AND u.var_id       <=> pu.var_id
  AND u.msg_ln       <=> pu.msg_ln
ORDER BY situacion, iss_cd, datapoint_id;


-- 2.4 Historial de incidencias de validación de un datapoint ---------------------
-- Desde cuándo falla y con qué código, en todas las validaciones.
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp)
SELECT v.crtn_dt AS validado_dt,
       v.vld_id,
       v.vld_stts AS veredicto_framework,
       i.iss_cd,
       NULLIF(i.var_id, '') AS variable,
       i.msg_ln
FROM dp_lgl_reg_dpm_iss_log i
JOIN dp_lgl_reg_dpm_param_vld v
  ON v.vld_id = i.vld_id
CROSS JOIN params p
WHERE COALESCE(i.vld_id, '') <> ''
  AND UPPER(v.framework_cd) = p.fw
  AND TRIM(i.datapoint_id) = p.dp
ORDER BY v.crtn_dt DESC, i.iss_cd;


/* =============================================================================
   3. ÚLTIMA EJECUCIÓN DEL MOTOR
   ============================================================================= */

-- 3.1 Resumen de la ejecución ------------------------------------------------------
WITH params AS (SELECT 'FCP' AS fw, '' AS periodo, '' AS exec_id),
ejec AS (
  SELECT e.*
  FROM dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND array_contains(split(UPPER(e.framework_list_ln), ','), p.fw)
         AND (p.periodo = '' OR date_format(e.ref_dt, 'yyyyMM') = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
  LIMIT 1
)
SELECT e.exec_id,
       date_format(e.ref_dt, 'yyyyMM') AS periodo,
       e.exec_stts,
       e.param_snpsh_dt,
       e.start_dt,
       e.end_dt,
       e.msg_ln                        AS mensaje_ejecucion,
       i.sev_cd,
       i.iss_cd,
       COUNT(i.iss_id)                 AS ocurrencias,
       COUNT(DISTINCT NULLIF(i.datapoint_id, '')) AS datapoints_afectados,
       SUM(i.afct_nr)                  AS elementos_afectados,
       MIN(i.msg_ln)                   AS ejemplo
FROM ejec e
LEFT JOIN dp_lgl_reg_dpm_iss_log i ON i.exec_id = e.exec_id
GROUP BY e.exec_id, e.ref_dt, e.exec_stts, e.param_snpsh_dt, e.start_dt, e.end_dt, e.msg_ln,
         i.sev_cd, i.iss_cd
ORDER BY i.sev_cd, ocurrencias DESC;


-- 3.2 Detalle de incidencias de la ejecución -----------------------------------------
-- Con el código de variable (V1, V2...) y, en los TECH, el error técnico original.
WITH params AS (SELECT 'FCP' AS fw, '' AS periodo, '' AS exec_id, '' AS dp),
ejec AS (
  SELECT e.*
  FROM dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND array_contains(split(UPPER(e.framework_list_ln), ','), p.fw)
         AND (p.periodo = '' OR date_format(e.ref_dt, 'yyyyMM') = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
  LIMIT 1
)
SELECT i.sev_cd,
       i.iss_cd,
       NULLIF(i.datapoint_id, '')        AS datapoint_id,
       COALESCE(lv.var_cd,
                CASE WHEN i.var_id LIKE 'CALC_%' THEN 'RESULT' END,
                NULLIF(i.var_id, ''))    AS variable,
       NULLIF(i.src_tbl_id, '')          AS tabla,
       i.afct_nr,
       i.msg_ln,
       NULLIF(i.grp_id, '')              AS grupo,
       NULLIF(i.raw_err_cla_nm, '')      AS error_tecnico,
       NULLIF(i.raw_msg_ln, '')          AS detalle_tecnico,
       i.exec_id
FROM dp_lgl_reg_dpm_iss_log i
JOIN ejec e ON i.exec_id = e.exec_id
LEFT JOIN dp_lgl_reg_dpm_lineage_var lv
       ON lv.exec_id = i.exec_id AND lv.var_id = i.var_id
CROSS JOIN params p
WHERE p.dp = '' OR TRIM(i.datapoint_id) = p.dp
ORDER BY i.sev_cd, i.iss_cd, datapoint_id, variable;


-- 3.3 Pasos y tiempos de la ejecución ---------------------------------------------
-- Los grupos BASE más lentos son los candidatos a optimizar.
WITH params AS (SELECT 'FCP' AS fw, '' AS periodo, '' AS exec_id),
ejec AS (
  SELECT e.*
  FROM dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND array_contains(split(UPPER(e.framework_list_ln), ','), p.fw)
         AND (p.periodo = '' OR date_format(e.ref_dt, 'yyyyMM') = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
  LIMIT 1
)
SELECT s.step_nm,
       NULLIF(s.grp_id, '')                                           AS grupo,
       s.step_stts,
       s.row_nr,
       ROUND(unix_timestamp(s.end_dt) - unix_timestamp(s.start_dt), 0) AS segundos,
       s.msg_ln,
       s.start_dt,
       s.end_dt
FROM dp_lgl_reg_dpm_exec_step s
JOIN ejec e ON s.exec_id = e.exec_id
ORDER BY segundos DESC;


/* =============================================================================
   4. UN DATAPOINT EN UNA EJECUCIÓN
   (la ejecución se elige igual que en el bloque 3: exec_id, o la última del
    framework y periodo)
   ============================================================================= */

-- 4.1 Cabecera del datapoint ---------------------------------------------------------
-- Tipo, fórmula, si se consolida, SQL que construye sus filas (datapoint_sql_ln),
-- nº de filas calculadas y publicadas, e incidencias de la ejecución.
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp, '' AS periodo, '' AS exec_id),
ejec AS (
  SELECT e.*
  FROM dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND array_contains(split(UPPER(e.framework_list_ln), ','), p.fw)
         AND (p.periodo = '' OR date_format(e.ref_dt, 'yyyyMM') = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
  LIMIT 1
),
ld AS (
  SELECT l.*
  FROM dp_lgl_reg_dpm_lineage_datapoint l
  JOIN ejec e ON l.exec_id = e.exec_id
  CROSS JOIN params p
  WHERE TRIM(l.datapoint_id) = p.dp
),
res AS (
  SELECT COUNT(*) AS filas_calculadas, SUM(r.contributing_row_nr) AS registros_origen
  FROM dp_lgl_reg_dpm_result_datapoint_val r
  JOIN ejec e ON r.exec_id = e.exec_id
  CROSS JOIN params p
  WHERE TRIM(r.datapoint_id) = p.dp
),
pub AS (
  SELECT COUNT(*) AS filas_publicadas
  FROM dp_lgl_reg_dpm_rpt_datapoint r
  JOIN ejec e ON r.exec_id = e.exec_id
  CROSS JOIN params p
  WHERE TRIM(r.datapoint_id) = p.dp
),
iss AS (
  SELECT concat_ws(', ', collect_set(concat(i.sev_cd, ' ', i.iss_cd))) AS incidencias
  FROM dp_lgl_reg_dpm_iss_log i
  JOIN ejec e ON i.exec_id = e.exec_id
  CROSS JOIN params p
  WHERE TRIM(i.datapoint_id) = p.dp
)
SELECT e.exec_id,
       date_format(e.ref_dt, 'yyyyMM') AS periodo,
       e.exec_stts,
       p.dp                            AS datapoint_id,
       ld.tmplt_cd, ld.row_cd, ld.column_cd,
       COALESCE(ld.datapoint_cl_typ, 'SIN LINAJE: no se calculó en esta ejecución') AS tipo,
       ld.fmla_ln,
       ld.csl_flg                      AS consolida_por_vistas,
       res.filas_calculadas,
       res.registros_origen,
       pub.filas_publicadas,
       CASE WHEN pub.filas_publicadas > 0 THEN 'PUBLICADO en rpt_datapoint'
            WHEN res.filas_calculadas > 0 THEN 'Calculado; rpt_datapoint tiene otra ejecución'
            ELSE 'NO CALCULADO' END    AS situacion,
       iss.incidencias,
       ld.var_map_json_ln,
       ld.datapoint_sql_ln
FROM ejec e
CROSS JOIN params p
LEFT JOIN ld ON TRUE
CROSS JOIN res
CROSS JOIN pub
CROSS JOIN iss;


-- 4.2 Variables del datapoint: SELECT ejecutado, importe e issue code -------------
-- Una fila por variable (en un Rollup, las variables de sus componentes, con su
-- signo). src_sql_ln es el SELECT exacto que calcula la variable; sql_detalle es
-- el mismo SELECT sin agregar, para ver los registros de origen (copiarlo a otra
-- celda). Ojo: sql_detalle no aplica el HAVING.
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp, '' AS periodo, '' AS exec_id),
ejec AS (
  SELECT e.*
  FROM dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND array_contains(split(UPPER(e.framework_list_ln), ','), p.fw)
         AND (p.periodo = '' OR date_format(e.ref_dt, 'yyyyMM') = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
  LIMIT 1
),
vars AS (
  SELECT l.exec_id, l.datapoint_id AS datapoint_pedido, v.*
  FROM dp_lgl_reg_dpm_lineage_datapoint l
  JOIN ejec e ON l.exec_id = e.exec_id
  CROSS JOIN params p
  LATERAL VIEW explode(from_json(l.var_map_json_ln,
      'array<struct<datapoint_id:string,var_cd:string,var_id:string,sign:int>>')) t AS v
  WHERE TRIM(l.datapoint_id) = p.dp
),
val AS (
  SELECT r.var_id,
         SUM(r.val_amnt)            AS importe_total_claves,
         COUNT(*)                   AS claves_con_valor,
         SUM(r.contributing_row_nr) AS registros_origen
  FROM dp_lgl_reg_dpm_result_var_val r
  JOIN ejec e ON r.exec_id = e.exec_id
  WHERE r.var_id IN (SELECT var_id FROM vars)
  GROUP BY r.var_id
),
fk AS (
  SELECT f.var_id, COUNT(*) AS claves_fallidas,
         concat_ws(', ', collect_set(f.iss_cd)) AS motivo_claves_fallidas
  FROM dp_lgl_reg_dpm_failed_var_key f
  JOIN ejec e ON f.exec_id = e.exec_id
  WHERE f.var_id IN (SELECT var_id FROM vars)
  GROUP BY f.var_id
),
iss AS (
  SELECT i.var_id,
         concat_ws(', ', collect_set(i.iss_cd)) AS issue_cd,
         MIN(i.msg_ln)                          AS issue_msg
  FROM dp_lgl_reg_dpm_iss_log i
  JOIN ejec e ON i.exec_id = e.exec_id
  WHERE i.var_id IN (SELECT var_id FROM vars)
  GROUP BY i.var_id
)
SELECT v.datapoint_pedido,
       v.datapoint_id          AS datapoint_de_la_variable,
       v.var_cd,
       v.sign                  AS signo,
       lv.src_tbl_id,
       lv.agg_fcn_cd,
       lv.metric_fmla_ln,
       lv.having_ln,
       lv.per_cd               AS periodo_leido,
       val.importe_total_claves,
       val.claves_con_valor,
       val.registros_origen,
       fk.claves_fallidas,
       fk.motivo_claves_fallidas,
       iss.issue_cd,
       iss.issue_msg,
       lv.src_sql_ln,
       regexp_replace(
         regexp_replace(lv.src_sql_ln, '(?s)^SELECT .*?\\nFROM ', 'SELECT *\nFROM '),
         '(?s)\\nGROUP BY .*$', '')  AS sql_detalle,
       v.var_id
FROM vars v
LEFT JOIN dp_lgl_reg_dpm_lineage_var lv ON lv.exec_id = v.exec_id AND lv.var_id = v.var_id
LEFT JOIN val ON val.var_id = v.var_id
LEFT JOIN fk  ON fk.var_id  = v.var_id
LEFT JOIN iss ON iss.var_id = v.var_id
ORDER BY v.datapoint_id, v.var_cd;


-- 4.3 Importes del datapoint por vista y entidad, con el valor publicado ----------
-- result_datapoint_val (valor numérico de la ejecución) frente a rpt_datapoint
-- (valor formateado que recibe Regnology). Incluye los Default (datapoint_def).
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp, '' AS periodo, '' AS exec_id),
ejec AS (
  SELECT e.*
  FROM dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND array_contains(split(UPPER(e.framework_list_ln), ','), p.fw)
         AND (p.periodo = '' OR date_format(e.ref_dt, 'yyyyMM') = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
  LIMIT 1
),
calc AS (
  SELECT r.exec_id, r.datapoint_id, r.csl_view_cd, r.csl_perim_typ, r.csl_ent_cd,
         r.datapoint_typ, r.ctry_cd, r.val_amnt, CAST(NULL AS STRING) AS valor_default,
         r.contributing_row_nr, r.key_json_ln
  FROM dp_lgl_reg_dpm_result_datapoint_val r
  JOIN ejec e ON r.exec_id = e.exec_id
  CROSS JOIN params p
  WHERE TRIM(r.datapoint_id) = p.dp
  UNION ALL
  SELECT d.exec_id, d.datapoint_id, d.csl_view_cd, d.csl_perim_typ, d.csl_ent_cd,
         d.datapoint_typ, d.ctry_cd, CAST(NULL AS DECIMAL(38,10)), d.def_val_ln,
         CAST(NULL AS BIGINT), CAST(NULL AS STRING)
  FROM dp_lgl_reg_dpm_datapoint_def d
  JOIN ejec e ON d.exec_id = e.exec_id
  CROSS JOIN params p
  WHERE TRIM(d.datapoint_id) = p.dp
)
SELECT c.csl_view_cd,
       c.csl_perim_typ,
       c.csl_ent_cd,
       c.datapoint_typ,
       c.ctry_cd,
       c.val_amnt,
       c.valor_default,
       c.contributing_row_nr AS registros_origen,
       r.rpt_val_ln          AS valor_publicado,
       r.pbh_dt              AS publicado_dt,
       c.key_json_ln         AS clave
FROM calc c
LEFT JOIN dp_lgl_reg_dpm_rpt_datapoint r
       ON r.exec_id = c.exec_id AND r.datapoint_id = c.datapoint_id
      AND r.csl_view_cd   <=> c.csl_view_cd
      AND r.csl_perim_typ <=> c.csl_perim_typ
      AND r.csl_ent_cd    <=> c.csl_ent_cd
      AND r.datapoint_typ <=> c.datapoint_typ
      AND r.ctry_cd       <=> c.ctry_cd
ORDER BY c.csl_view_cd, c.csl_ent_cd, c.datapoint_typ, c.ctry_cd;


-- 4.4 Importe de cada variable por clave (entidad) y claves fallidas --------------
-- Incluye la variable RESULT de los Calculated con fórmula. Las claves fallidas
-- solo tienen el hash de la clave (key_hsh_cd), no su JSON.
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp, '' AS periodo, '' AS exec_id),
ejec AS (
  SELECT e.*
  FROM dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND array_contains(split(UPPER(e.framework_list_ln), ','), p.fw)
         AND (p.periodo = '' OR date_format(e.ref_dt, 'yyyyMM') = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
  LIMIT 1
),
var_ids AS (
  SELECT v.var_id
  FROM dp_lgl_reg_dpm_lineage_datapoint l
  JOIN ejec e ON l.exec_id = e.exec_id
  CROSS JOIN params p
  LATERAL VIEW explode(from_json(l.var_map_json_ln,
      'array<struct<datapoint_id:string,var_cd:string,var_id:string,sign:int>>')) t AS v
  WHERE TRIM(l.datapoint_id) = p.dp
)
SELECT r.datapoint_id,
       r.var_cd,
       r.var_cl_typ,
       get_json_object(r.key_json_ln, '$.ENTIDADE') AS entidad,
       get_json_object(r.key_json_ln, '$.IDENT')    AS ident,
       get_json_object(r.key_json_ln, '$.IDSUBENT') AS subentidad,
       get_json_object(r.key_json_ln, '$.CDWA')     AS pais,
       r.val_amnt,
       r.contributing_row_nr AS registros_origen,
       'OK'                  AS estado,
       r.key_hsh_cd
FROM dp_lgl_reg_dpm_result_var_val r
JOIN ejec e ON r.exec_id = e.exec_id
CROSS JOIN params p
WHERE r.var_id IN (SELECT var_id FROM var_ids)
   OR (TRIM(r.datapoint_id) = p.dp AND r.var_cl_typ = 'DERIVED')
UNION ALL
SELECT f.datapoint_id,
       COALESCE(lv.var_cd, CASE WHEN f.var_id LIKE 'CALC_%' THEN 'RESULT' END),
       CASE WHEN f.var_id LIKE 'CALC_%' THEN 'DERIVED' ELSE 'BASE' END,
       NULL, NULL, NULL, NULL,
       CAST(NULL AS DECIMAL(38,10)),
       CAST(NULL AS BIGINT),
       concat('FALLIDA: ', f.iss_cd),
       f.key_hsh_cd
FROM dp_lgl_reg_dpm_failed_var_key f
JOIN ejec e ON f.exec_id = e.exec_id
LEFT JOIN dp_lgl_reg_dpm_lineage_var lv ON lv.exec_id = f.exec_id AND lv.var_id = f.var_id
CROSS JOIN params p
WHERE f.var_id IN (SELECT var_id FROM var_ids) OR TRIM(f.datapoint_id) = p.dp
ORDER BY var_cd, entidad, ident, subentidad;


/* =============================================================================
   5. OTRAS UTILIDADES
   ============================================================================= */

-- 5.1 Qué cambió en la última carga de un framework ----------------------------------
-- Filas nuevas, modificadas o retiradas en la última carga que tocó los ficheros del
-- framework, con el contenido antes y después. Las versiones se comparan por
-- contenido: una fila modificada es una versión cerrada y otra nueva en el mismo
-- instante; se emparejan por pestaña, datapoint, variable y columna del filtro.
WITH params AS (SELECT 'FCP' AS fw),
ficheros AS (
  SELECT DISTINCT src_file_nm
  FROM dp_lgl_reg_dpm_param_datapoint d
  JOIN params p ON UPPER(d.framework_cd) = p.fw
),
filas AS (
  SELECT 'Datapoints' AS pestana, src_file_nm, datapoint_id AS objeto, var_cd,
         CAST(NULL AS STRING) AS columna,
         concat_ws(' | ', datapoint_cl_typ, src_tbl_id, metric_fmla_ln, having_ln, per_expr_cd) AS contenido,
         src_row_nr, ref_start_dt, ref_end_dt, row_present_flg
  FROM dp_lgl_reg_dpm_param_datapoint
  UNION ALL
  SELECT 'Filters', src_file_nm, datapoint_id, var_cd, UPPER(src_column_nm),
         concat_ws(' | ', src_column_nm, optr_cd, fltr_val_ln),
         src_row_nr, ref_start_dt, ref_end_dt, row_present_flg
  FROM dp_lgl_reg_dpm_param_fltr
  UNION ALL
  SELECT 'Catalog', src_file_nm, datapoint_id, CAST(NULL AS STRING), CAST(NULL AS STRING),
         concat_ws(' | ', tmplt_cd, row_cd, column_cd, val_typ),
         src_row_nr, ref_start_dt, ref_end_dt, row_present_flg
  FROM dp_lgl_reg_dpm_param_ctlg
  UNION ALL
  SELECT 'Sources', src_file_nm, src_tbl_id, CAST(NULL AS STRING), CAST(NULL AS STRING),
         concat_ws(' | ', phy_tbl_nm, per_column_nm, per_fmt_cd, ent_column_nm,
                          ident_column_nm, subent_column_nm, ctry_column_nm, grain_id),
         src_row_nr, ref_start_dt, ref_end_dt, row_present_flg
  FROM dp_lgl_reg_dpm_param_src_tbl
),
fw_filas AS (
  SELECT f.* FROM filas f JOIN ficheros fi ON fi.src_file_nm = f.src_file_nm
),
ts AS (SELECT greatest(MAX(ref_start_dt), MAX(ref_end_dt)) AS ts FROM fw_filas),
nuevas   AS (SELECT f.* FROM fw_filas f JOIN ts ON f.ref_start_dt = ts.ts WHERE f.row_present_flg = 1),
cerradas AS (SELECT f.* FROM fw_filas f JOIN ts ON f.ref_end_dt   = ts.ts WHERE f.row_present_flg = 1)
SELECT (SELECT ts FROM ts)                         AS carga_dt,
       COALESCE(n.pestana, c.pestana)              AS pestana,
       COALESCE(n.objeto, c.objeto)                AS objeto,
       COALESCE(n.var_cd, c.var_cd)                AS var_cd,
       CASE WHEN c.pestana IS NULL THEN 'NUEVA'
            WHEN n.pestana IS NULL THEN 'RETIRADA'
            ELSE 'MODIFICADA' END                  AS cambio,
       c.contenido                                 AS antes,
       n.contenido                                 AS despues,
       c.src_row_nr                                AS fila_excel_antes,
       n.src_row_nr                                AS fila_excel_despues,
       COALESCE(n.src_file_nm, c.src_file_nm)      AS src_file_nm
FROM nuevas n
FULL OUTER JOIN cerradas c
  ON  c.pestana = n.pestana AND c.src_file_nm = n.src_file_nm
  AND c.objeto <=> n.objeto AND c.var_cd <=> n.var_cd AND c.columna <=> n.columna
WHERE n.pestana IS NULL OR c.pestana IS NULL OR NOT (n.contenido <=> c.contenido)
ORDER BY pestana, objeto, var_cd;


-- 5.2 Datapoints del catálogo no publicados en un periodo y su motivo -----------------
-- Cruza el catálogo vigente con rpt_datapoint y busca la causa en la última
-- ejecución del periodo: incidencias del propio datapoint o tabla origen sin
-- datos del periodo (CALC.INPUT.NO_PERIOD_ROWS).
WITH params AS (SELECT 'FCP' AS fw, '202512' AS periodo),
ejec AS (
  SELECT e.*
  FROM dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE array_contains(split(UPPER(e.framework_list_ln), ','), p.fw)
    AND date_format(e.ref_dt, 'yyyyMM') = p.periodo
  ORDER BY e.start_dt DESC
  LIMIT 1
),
cat AS (
  SELECT c.src_file_nm, c.datapoint_id, c.tmplt_cd, c.row_cd, c.column_cd
  FROM dp_lgl_reg_dpm_param_ctlg c
  JOIN params p ON UPPER(c.framework_cd) = p.fw
  WHERE c.ctrl_state = 1 AND c.row_present_flg = 1
),
pub AS (
  SELECT DISTINCT r.datapoint_id
  FROM dp_lgl_reg_dpm_rpt_datapoint r
  CROSS JOIN params p
  WHERE UPPER(r.framework_cd) = p.fw AND date_format(r.ref_dt, 'yyyyMM') = p.periodo
),
tipo AS (
  SELECT d.src_file_nm, d.datapoint_id,
         concat_ws(', ', collect_set(d.datapoint_cl_typ)) AS tipo,
         collect_set(s.phy_tbl_nm)                        AS tablas
  FROM dp_lgl_reg_dpm_param_datapoint d
  LEFT JOIN dp_lgl_reg_dpm_param_src_tbl s
         ON s.src_file_nm = d.src_file_nm AND UPPER(s.src_tbl_id) = UPPER(d.src_tbl_id)
        AND s.ctrl_state = 1 AND s.row_present_flg = 1
  WHERE d.ctrl_state = 1 AND d.row_present_flg = 1
  GROUP BY d.src_file_nm, d.datapoint_id
),
iss_dp AS (
  SELECT i.datapoint_id, concat_ws(', ', collect_set(i.iss_cd)) AS incidencias
  FROM dp_lgl_reg_dpm_iss_log i JOIN ejec e ON i.exec_id = e.exec_id
  GROUP BY i.datapoint_id
),
sin_periodo AS (
  SELECT DISTINCT UPPER(i.src_tbl_id) AS tabla
  FROM dp_lgl_reg_dpm_iss_log i JOIN ejec e ON i.exec_id = e.exec_id
  WHERE i.iss_cd = 'CALC.INPUT.NO_PERIOD_ROWS'
),
dp_sin_periodo AS (
  SELECT DISTINCT x.src_file_nm, x.datapoint_id
  FROM (SELECT t.src_file_nm, t.datapoint_id, explode(t.tablas) AS tabla FROM tipo t) x
  JOIN sin_periodo sp ON sp.tabla = UPPER(x.tabla)
)
SELECT c.datapoint_id,
       c.tmplt_cd, c.row_cd, c.column_cd,
       t.tipo,
       (SELECT MAX(exec_id) FROM ejec)  AS ultima_ejecucion_periodo,
       (SELECT MAX(exec_stts) FROM ejec) AS estado_ejecucion,
       COALESCE(i.incidencias,
                CASE WHEN sp.datapoint_id IS NOT NULL
                     THEN 'CALC.INPUT.NO_PERIOD_ROWS (tabla origen sin datos del periodo)' END,
                'Sin incidencia: revisar si es nuevo en el catálogo o si sus componentes no tienen valor') AS motivo
FROM cat c
LEFT JOIN pub  p ON p.datapoint_id = c.datapoint_id
LEFT JOIN tipo t ON t.src_file_nm = c.src_file_nm AND t.datapoint_id = c.datapoint_id
LEFT JOIN iss_dp i ON i.datapoint_id = c.datapoint_id
LEFT JOIN dp_sin_periodo sp ON sp.src_file_nm = c.src_file_nm AND sp.datapoint_id = c.datapoint_id
WHERE p.datapoint_id IS NULL
ORDER BY c.tmplt_cd, c.row_cd, c.column_cd;


-- 5.3 Comparar los importes de dos ejecuciones ---------------------------------------
-- Por defecto, las dos últimas ejecuciones terminadas del framework y periodo.
-- Útil tras un reproceso o un cambio de parametría. umbral = diferencia mínima.
WITH params AS (SELECT 'FCP' AS fw, '202512' AS periodo,
                       '' AS exec_anterior, '' AS exec_nueva, 0.01 AS umbral),
ej AS (
  SELECT e.exec_id, ROW_NUMBER() OVER (ORDER BY e.start_dt DESC) AS rn
  FROM dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE array_contains(split(UPPER(e.framework_list_ln), ','), p.fw)
    AND date_format(e.ref_dt, 'yyyyMM') = p.periodo
    AND e.exec_stts NOT IN ('FAILED', 'RUNNING')
),
sel AS (
  SELECT COALESCE(NULLIF(p.exec_anterior, ''), MAX(CASE WHEN ej.rn = 2 THEN ej.exec_id END)) AS a,
         COALESCE(NULLIF(p.exec_nueva, ''),    MAX(CASE WHEN ej.rn = 1 THEN ej.exec_id END)) AS b
  FROM params p LEFT JOIN ej ON TRUE
  GROUP BY p.exec_anterior, p.exec_nueva
),
a AS (SELECT r.* FROM dp_lgl_reg_dpm_result_datapoint_val r JOIN sel ON r.exec_id = sel.a),
b AS (SELECT r.* FROM dp_lgl_reg_dpm_result_datapoint_val r JOIN sel ON r.exec_id = sel.b)
SELECT COALESCE(a.datapoint_id, b.datapoint_id) AS datapoint_id,
       COALESCE(a.csl_view_cd,  b.csl_view_cd)  AS csl_view_cd,
       COALESCE(a.csl_ent_cd,   b.csl_ent_cd)   AS csl_ent_cd,
       COALESCE(a.datapoint_typ, b.datapoint_typ) AS datapoint_typ,
       COALESCE(a.ctry_cd,      b.ctry_cd)      AS ctry_cd,
       a.val_amnt                               AS valor_anterior,
       b.val_amnt                               AS valor_nuevo,
       COALESCE(b.val_amnt, 0) - COALESCE(a.val_amnt, 0) AS diferencia,
       CASE WHEN a.datapoint_id IS NULL THEN 'SOLO EN LA NUEVA'
            WHEN b.datapoint_id IS NULL THEN 'SOLO EN LA ANTERIOR'
            ELSE 'DISTINTO' END                 AS situacion,
       (SELECT a FROM sel) AS exec_anterior,
       (SELECT b FROM sel) AS exec_nueva
FROM a
FULL OUTER JOIN b
  ON a.datapoint_id = b.datapoint_id AND a.key_hsh_cd = b.key_hsh_cd
CROSS JOIN params p
WHERE a.datapoint_id IS NULL OR b.datapoint_id IS NULL
   OR ABS(COALESCE(b.val_amnt, 0) - COALESCE(a.val_amnt, 0)) >= p.umbral
ORDER BY ABS(COALESCE(b.val_amnt, 0) - COALESCE(a.val_amnt, 0)) DESC;


-- 5.4 Qué datapoints usan una tabla, columna o valor de filtro ---------------------
-- Análisis de impacto antes de tocar una tabla origen o una cuenta.
-- Cualquier parámetro vacío no filtra. fw = '' busca en todos los frameworks.
WITH params AS (SELECT '' AS fw, '' AS tabla, 'NATACCOUNT' AS columna, '1001' AS valor),
d AS (
  SELECT * FROM dp_lgl_reg_dpm_param_datapoint
  WHERE ctrl_state = 1 AND row_present_flg = 1
),
f AS (
  SELECT * FROM dp_lgl_reg_dpm_param_fltr
  WHERE ctrl_state = 1 AND row_present_flg = 1
)
SELECT d.framework_cd,
       d.datapoint_id,
       COALESCE(NULLIF(d.var_cd, ''), '(única)') AS variable,
       d.datapoint_cl_typ,
       d.src_tbl_id,
       d.metric_fmla_ln,
       f.src_column_nm,
       f.optr_cd,
       f.fltr_val_ln
FROM d
LEFT JOIN f
       ON f.src_file_nm = d.src_file_nm AND f.datapoint_id = d.datapoint_id
      AND UPPER(COALESCE(f.var_cd, '')) = UPPER(COALESCE(d.var_cd, ''))
CROSS JOIN params p
WHERE (p.fw = ''      OR UPPER(d.framework_cd) = p.fw)
  AND (p.tabla = ''   OR UPPER(d.src_tbl_id) = UPPER(p.tabla))
  AND (p.columna = '' OR UPPER(f.src_column_nm) = UPPER(p.columna)
                      OR UPPER(d.metric_fmla_ln) LIKE concat('%', UPPER(p.columna), '%'))
  AND (p.valor = ''   OR UPPER(f.fltr_val_ln) LIKE concat('%', UPPER(p.valor), '%'))
ORDER BY d.framework_cd, d.datapoint_id, variable;


-- 5.5 Ejecuciones colgadas en RUNNING --------------------------------------------------
-- RUNNING sin avance en exec_step desde hace más de N minutos: según la Guía,
-- tratarlas como FAILED y repetir el cálculo.
WITH params AS (SELECT 30 AS minutos_sin_avance)
SELECT e.exec_id,
       e.framework_list_ln,
       date_format(e.ref_dt, 'yyyyMM')         AS periodo,
       e.start_dt,
       MAX(s.end_dt)                           AS ultimo_paso_dt,
       max_by(s.step_nm, s.end_dt)             AS ultimo_paso
FROM dp_lgl_reg_dpm_exec e
LEFT JOIN dp_lgl_reg_dpm_exec_step s ON s.exec_id = e.exec_id
CROSS JOIN params p
WHERE e.exec_stts = 'RUNNING'
GROUP BY e.exec_id, e.framework_list_ln, e.ref_dt, e.start_dt, p.minutos_sin_avance
HAVING COALESCE(MAX(s.end_dt), e.start_dt)
       < current_timestamp() - make_interval(0, 0, 0, 0, 0, p.minutos_sin_avance, 0)
ORDER BY e.start_dt;


-- 5.6 Significado de un código de incidencia --------------------------------------------
-- Busca por código o parte del código. La resolución recomendada está en el
-- anexo B de la Guía práctica.
WITH params AS (SELECT 'COLUMN_NOT_FOUND' AS codigo)
SELECT c.iss_cd, c.dmn_cd, c.stg_cd, c.sev_cd, c.scp_cd, c.act_flg,
       c.msg_tmplt_ln, c.param_nm_list_ln
FROM dp_lgl_reg_dpm_param_iss_ctlg c
CROSS JOIN params p
WHERE c.ctrl_state = 1 AND c.row_present_flg = 1
  AND UPPER(c.iss_cd) LIKE concat('%', UPPER(p.codigo), '%')
ORDER BY c.iss_cd;


-- 5.7 Incidencias más frecuentes de los últimos 30 días -------------------------------
-- Separadas por fase (VALIDATION / PROCESSING). Ayuda a ver qué errores de
-- parametría se repiten y merecen una regla en la pestaña Validation del Excel.
WITH params AS (SELECT '' AS fw, 30 AS dias)
SELECT i.phs_cd,
       UPPER(i.framework_cd)          AS framework_cd,
       i.sev_cd,
       i.iss_cd,
       COUNT(*)                       AS ocurrencias,
       COUNT(DISTINCT COALESCE(NULLIF(i.vld_id, ''), i.exec_id)) AS procesos,
       COUNT(DISTINCT NULLIF(i.datapoint_id, '')) AS datapoints,
       MAX(i.crtn_dt)                 AS ultima_vez
FROM dp_lgl_reg_dpm_iss_log i
CROSS JOIN params p
WHERE i.crtn_dt >= current_timestamp() - make_interval(0, 0, 0, p.dias, 0, 0, 0)
  AND (p.fw = '' OR UPPER(i.framework_cd) = p.fw)
GROUP BY i.phs_cd, UPPER(i.framework_cd), i.sev_cd, i.iss_cd
ORDER BY ocurrencias DESC;


-- 5.8 Últimas subidas de parametría (DataLoader) ---------------------------------------
-- Una fila por subida: fichero, quién la subió, estado de la promoción y veredicto
-- de su validación. fw = '' para ver todos los frameworks.
WITH params AS (SELECT 'FCP' AS fw)
SELECT l.crtn_dt,
       l.framework_cd,
       l.src_file_nm,
       l.load_usr_id,
       l.load_sts_cd,
       v.vld_stts,
       v.err_nr,
       l.param_snpsh_dt,
       l.vld_id,
       l.datapoint_load_id, l.fltr_load_id, l.ctlg_load_id, l.src_load_id
FROM dp_lgl_reg_dpm_param_load l
LEFT JOIN dp_lgl_reg_dpm_param_vld v
       ON v.vld_id = l.vld_id AND UPPER(v.framework_cd) = UPPER(l.framework_cd)
CROSS JOIN params p
WHERE p.fw = '' OR UPPER(l.framework_cd) = p.fw
ORDER BY l.crtn_dt DESC
LIMIT 20;


/* =============================================================================
   ANEXO · RE-EJECUTAR EL LINAJE DE UN DATAPOINT DESDE PYSPARK
   -----------------------------------------------------------------------------
   El SQL no puede ejecutar texto dinámico, así que para lanzar de golpe los
   SELECT guardados en lineage_var se usa una celda Python. Comprueba que cada
   variable reproduce el importe calculado por el motor (diferencia = 0) y deja
   en `detalle` los registros de origen de la primera variable.

   exec_id = "<exec_id de la consulta 3.1>"
   dp      = "T02.00,i0240"

   vars = spark.sql(f"""
       SELECT v.var_id, v.var_cd, lv.src_sql_ln
       FROM dp_lgl_reg_dpm_lineage_datapoint l
       LATERAL VIEW explode(from_json(l.var_map_json_ln,
           'array<struct<datapoint_id:string,var_cd:string,var_id:string,sign:int>>')) t AS v
       JOIN dp_lgl_reg_dpm_lineage_var lv
         ON lv.exec_id = l.exec_id AND lv.var_id = v.var_id
       WHERE l.exec_id = '{exec_id}' AND l.datapoint_id = '{dp}'
   """).collect()

   for row in vars:
       relanzado = spark.sql(row.src_sql_ln).agg({"val_amnt": "sum"}).first()[0]
       motor = spark.sql(f"""SELECT SUM(val_amnt) FROM dp_lgl_reg_dpm_result_var_val
                             WHERE exec_id = '{exec_id}' AND var_id = '{row.var_id}'""").first()[0]
       print(row.var_cd, "relanzado:", relanzado, "motor:", motor,
             "diferencia:", (relanzado or 0) - (motor or 0))

   Si la diferencia no es 0, la causa suele ser una clave fallida (consulta 4.4),
   que el motor excluye y el SELECT relanzado sí suma.
   ============================================================================= */
