/* =============================================================================
   MOTOR DPM · CONSULTAS DEL DÍA A DÍA · VERSIÓN T-SQL
   -----------------------------------------------------------------------------
   Dónde se ejecutan : SQL analytics endpoint del Lakehouse
                       DP_FET_LH_LegalAndRegulatoryReporting (editor de consultas
                       de Fabric, SSMS o Azure Data Studio). Mismas consultas y
                       misma numeración que la versión para notebook.
   Modelo de datos   : tablas dp_lgl_reg_dpm_* tal como las define
                       DE_DP_NB_LGL_REG_CREATE_TABLES (repo, 20/11/2026).
   Fechas            : todas las marcas de tiempo del motor están en UTC.

   CÓMO USARLAS
   Selecciona una consulta completa (desde su WITH hasta el punto y coma) y
   ejecútala. Cada una empieza con  WITH params AS (...) : cambia solo esa línea.
       fw       framework en mayúsculas          'FCP', 'FINREP'...
       dp       ID Datapoint tal cual en Excel   'T02.00,i0240'
       periodo  AAAAMM  ('' = cualquiera)        '202512'
       exec_id  ''  = última ejecución terminada (se ignoran las RUNNING)
       vld_id   ''  = última validación del framework

   DIFERENCIAS CON EL NOTEBOOK QUE CONVIENE CONOCER
   1. El endpoint es de solo lectura y se sincroniza con Delta con un pequeño
      retraso: una ejecución recién terminada puede tardar unos instantes en
      verse aquí.
   2. Las columnas de texto de Delta se exponen como varchar(8000). Un
      src_sql_ln, datapoint_sql_ln o var_map_json_ln más largo (listas IN muy
      grandes, Rollups con muchos componentes) puede llegar truncado: para esos
      casos, usar la versión notebook.
   3. Los SELECT del linaje (src_sql_ln) están escritos en Spark SQL. La 4.2
      devuelve además una traducción automática a T-SQL (sql_tsql y
      sql_detalle_tsql) que cubre la sintaxis que genera el motor: comillas
      invertidas, CAST AS STRING, literales DATE'...' y nombres de tabla del
      Lakehouse. Las filas creadas antes de la pestaña Sources usan
      REGEXP_REPLACE en el filtro de periodo y no se traducen.
   4. La intercalación del endpoint distingue mayúsculas en los datos: por eso
      las comparaciones de framework y tipo van con UPPER().

   Recordatorio del modelo (igual que en el notebook)
   - Parametría vigente = ctrl_state = 1 AND row_present_flg = 1.
   - En iss_log, las incidencias de validación llevan vld_id (exec_id vacío) y
     las de ejecución, exec_id (vld_id vacío).
   - En iss_log, var_id es V1, V2... en la validación y un identificador interno
     (VAR_xxx / CALC_xxx) en la ejecución.
   ============================================================================= */


/* =============================================================================
   0. PANORAMA
   ============================================================================= */

-- 0.1 Estado de cada framework ------------------------------------------------
WITH params AS (SELECT '' AS fw),   -- '' = todos los frameworks
fw_files AS (
  SELECT DISTINCT UPPER(framework_cd) AS framework_cd, src_file_nm
  FROM dbo.dp_lgl_reg_dpm_param_datapoint
  WHERE ctrl_state = 1 AND row_present_flg = 1
),
param_all AS (
  SELECT src_file_nm, ref_start_dt, ref_end_dt, ref_last_dt, ctrl_state FROM dbo.dp_lgl_reg_dpm_param_datapoint
  UNION ALL SELECT src_file_nm, ref_start_dt, ref_end_dt, ref_last_dt, ctrl_state FROM dbo.dp_lgl_reg_dpm_param_fltr
  UNION ALL SELECT src_file_nm, ref_start_dt, ref_end_dt, ref_last_dt, ctrl_state FROM dbo.dp_lgl_reg_dpm_param_ctlg
  UNION ALL SELECT src_file_nm, ref_start_dt, ref_end_dt, ref_last_dt, ctrl_state FROM dbo.dp_lgl_reg_dpm_param_src_tbl
),
param_estado AS (
  SELECT f.framework_cd,
         CASE WHEN MAX(a.ref_end_dt) > MAX(a.ref_start_dt)
              THEN MAX(a.ref_end_dt) ELSE MAX(a.ref_start_dt) END AS ultimo_cambio_dt,
         MAX(CASE WHEN a.ctrl_state = 1 THEN a.ref_last_dt END) AS ultima_carga_dt
  FROM fw_files f
  JOIN param_all a ON a.src_file_nm = f.src_file_nm
  GROUP BY f.framework_cd
),
ficheros AS (
  SELECT framework_cd, STRING_AGG(CAST(src_file_nm AS VARCHAR(MAX)), ', ') AS ficheros
  FROM fw_files
  GROUP BY framework_cd
),
n_dp AS (
  SELECT UPPER(framework_cd) AS framework_cd,
         COUNT(DISTINCT datapoint_id) AS datapoints,
         COUNT(*)                     AS filas_datapoints
  FROM dbo.dp_lgl_reg_dpm_param_datapoint
  WHERE ctrl_state = 1 AND row_present_flg = 1
  GROUP BY UPPER(framework_cd)
),
vld AS (
  SELECT * FROM (
    SELECT UPPER(framework_cd) AS framework_cd, vld_id, param_snpsh_dt, vld_stts, err_nr, crtn_dt,
           ROW_NUMBER() OVER (PARTITION BY UPPER(framework_cd) ORDER BY crtn_dt DESC) AS rn
    FROM dbo.dp_lgl_reg_dpm_param_vld
  ) t WHERE rn = 1
),
ult_valid AS (
  SELECT UPPER(framework_cd) AS framework_cd, MAX(param_snpsh_dt) AS ult_valid_snpsh_dt
  FROM dbo.dp_lgl_reg_dpm_param_vld
  WHERE vld_stts = 'VALID'
  GROUP BY UPPER(framework_cd)
),
ejec AS (
  SELECT * FROM (
    SELECT UPPER(TRIM(x.value)) AS framework_cd, e.exec_id, e.ref_dt, e.exec_stts,
           e.warning_nr, e.err_nr, e.start_dt, e.end_dt,
           ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(x.value)) ORDER BY e.start_dt DESC) AS rn
    FROM dbo.dp_lgl_reg_dpm_exec e
    CROSS APPLY STRING_SPLIT(e.framework_list_ln, ',') x
  ) t WHERE rn = 1
)
SELECT pe.framework_cd,
       fi.ficheros,
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
       CONVERT(CHAR(6), e.ref_dt, 112) AS periodo_ultima_ejecucion,
       e.exec_stts,
       e.warning_nr,
       e.err_nr,
       e.start_dt,
       e.end_dt
FROM param_estado pe
CROSS JOIN params p
LEFT JOIN ficheros fi  ON fi.framework_cd = pe.framework_cd
LEFT JOIN n_dp n       ON n.framework_cd  = pe.framework_cd
LEFT JOIN vld v        ON v.framework_cd  = pe.framework_cd
LEFT JOIN ult_valid uv ON uv.framework_cd = pe.framework_cd
LEFT JOIN ejec e       ON e.framework_cd  = pe.framework_cd
WHERE p.fw = '' OR pe.framework_cd = p.fw
ORDER BY pe.framework_cd;


-- 0.2 Últimas ejecuciones de un framework --------------------------------------
WITH params AS (SELECT 'FCP' AS fw, '' AS periodo)
SELECT TOP 20
       e.exec_id,
       e.req_id,
       CONVERT(CHAR(6), e.ref_dt, 112) AS periodo,
       e.framework_list_ln,
       e.exec_stts,
       e.warning_nr,
       e.err_nr,
       e.param_snpsh_dt,
       e.start_dt,
       e.end_dt,
       CAST(DATEDIFF(SECOND, e.start_dt, e.end_dt) / 60.0 AS DECIMAL(10,1)) AS minutos,
       e.msg_ln
FROM dbo.dp_lgl_reg_dpm_exec e
CROSS JOIN params p
WHERE CONCAT(',', UPPER(e.framework_list_ln), ',') LIKE CONCAT('%,', p.fw, ',%')
  AND (p.periodo = '' OR CONVERT(CHAR(6), e.ref_dt, 112) = p.periodo)
ORDER BY e.start_dt DESC;


-- 0.3 Últimas validaciones de un framework -------------------------------------
WITH params AS (SELECT 'FCP' AS fw)
SELECT TOP 20 v.vld_id, v.framework_cd, v.param_snpsh_dt, v.vld_stts, v.err_nr, v.crtn_dt
FROM dbo.dp_lgl_reg_dpm_param_vld v
CROSS JOIN params p
WHERE UPPER(v.framework_cd) = p.fw
ORDER BY v.crtn_dt DESC;


/* =============================================================================
   1. PARAMETRÍA VIGENTE DE UN DATAPOINT (última versión cargada)
   ============================================================================= */

-- 1.1 Ficha del datapoint (una fila por variable, con WHERE legible) -----------
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp),
d AS (
  SELECT d.*
  FROM dbo.dp_lgl_reg_dpm_param_datapoint d
  JOIN params p ON UPPER(d.framework_cd) = p.fw AND TRIM(d.datapoint_id) = p.dp
  WHERE d.ctrl_state = 1 AND d.row_present_flg = 1
),
f AS (
  SELECT f.src_file_nm, f.datapoint_id, UPPER(COALESCE(f.var_cd, '')) AS var_cd,
         STRING_AGG(CAST(CONCAT(f.src_column_nm, ' ', UPPER(f.optr_cd), ' ', f.fltr_val_ln) AS VARCHAR(MAX)), '  AND  ')
           WITHIN GROUP (ORDER BY f.src_column_nm, f.fltr_val_ln) AS where_legible,
         COUNT(*) AS n_filtros
  FROM dbo.dp_lgl_reg_dpm_param_fltr f
  JOIN (SELECT DISTINCT src_file_nm, datapoint_id FROM d) dd
    ON dd.src_file_nm = f.src_file_nm AND dd.datapoint_id = f.datapoint_id
  WHERE f.ctrl_state = 1 AND f.row_present_flg = 1
  GROUP BY f.src_file_nm, f.datapoint_id, UPPER(COALESCE(f.var_cd, ''))
)
SELECT d.framework_cd,
       d.src_file_nm,
       d.datapoint_id,
       c.tmplt_cd, c.row_cd, c.column_cd,
       COALESCE(NULLIF(c.val_typ, ''), 'Amount (por defecto)') AS val_typ,
       d.datapoint_cl_typ,
       COALESCE(NULLIF(d.var_cd, ''), CASE WHEN COALESCE(d.src_tbl_id, '') = '' THEN '(fórmula / expresión)' ELSE '(única)' END) AS variable,
       d.src_tbl_id,
       s.phy_tbl_nm,
       s.per_column_nm,
       s.per_fmt_cd,
       CONCAT_WS(' + ', NULLIF(s.ent_column_nm, ''), NULLIF(s.ident_column_nm, ''),
                        NULLIF(s.subent_column_nm, ''), NULLIF(s.ctry_column_nm, '')) AS clave_agrupacion,
       COALESCE(NULLIF(s.grain_id, ''), UPPER(d.src_tbl_id))   AS grain_id,
       d.metric_fmla_ln,
       d.having_ln,
       COALESCE(NULLIF(d.per_expr_cd, ''), CASE WHEN COALESCE(d.src_tbl_id, '') <> '' THEN 'M' END) AS periodo,
       f.n_filtros,
       f.where_legible,
       d.src_row_nr      AS fila_excel,
       d.ref_start_dt    AS version_desde,
       d.ref_last_dt     AS vista_en_ultima_carga
FROM d
LEFT JOIN dbo.dp_lgl_reg_dpm_param_ctlg c
       ON c.src_file_nm = d.src_file_nm AND c.datapoint_id = d.datapoint_id
      AND c.ctrl_state = 1 AND c.row_present_flg = 1
LEFT JOIN dbo.dp_lgl_reg_dpm_param_src_tbl s
       ON s.src_file_nm = d.src_file_nm AND UPPER(s.src_tbl_id) = UPPER(d.src_tbl_id)
      AND s.ctrl_state = 1 AND s.row_present_flg = 1
LEFT JOIN f
       ON f.src_file_nm = d.src_file_nm AND f.datapoint_id = d.datapoint_id
      AND f.var_cd = UPPER(COALESCE(d.var_cd, ''))
ORDER BY d.src_row_nr;


-- 1.2 Filtros del datapoint en detalle -----------------------------------------
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp),
ficheros AS (
  SELECT DISTINCT d.src_file_nm
  FROM dbo.dp_lgl_reg_dpm_param_datapoint d
  JOIN params p ON UPPER(d.framework_cd) = p.fw
  WHERE d.ctrl_state = 1 AND d.row_present_flg = 1
)
SELECT f.datapoint_id,
       COALESCE(NULLIF(f.var_cd, ''), '(única)') AS variable,
       f.src_column_nm,
       f.optr_cd,
       f.fltr_val_ln,
       LEN(f.fltr_val_ln) - LEN(REPLACE(f.fltr_val_ln, ',', '')) + 1 AS n_valores_aprox,
       f.src_row_nr                              AS fila_excel,
       f.ref_start_dt                            AS version_desde
FROM dbo.dp_lgl_reg_dpm_param_fltr f
JOIN ficheros fi ON fi.src_file_nm = f.src_file_nm
CROSS JOIN params p
WHERE f.ctrl_state = 1 AND f.row_present_flg = 1
  AND TRIM(f.datapoint_id) = p.dp
ORDER BY variable, fila_excel;


-- 1.3a Rollup: componentes del datapoint ---------------------------------------
-- Parte la expresión [A] + [B] - [C] por '[' y toma el signo del trozo anterior.
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0230' AS dp),
vig AS (
  SELECT * FROM dbo.dp_lgl_reg_dpm_param_datapoint
  WHERE ctrl_state = 1 AND row_present_flg = 1
),
r AS (
  SELECT v.src_file_nm, v.datapoint_id, v.metric_fmla_ln
  FROM vig v
  JOIN params p ON UPPER(v.framework_cd) = p.fw AND TRIM(v.datapoint_id) = p.dp
  WHERE UPPER(v.datapoint_cl_typ) = 'ROLLUP'
),
trozos AS (
  SELECT r.src_file_nm, r.datapoint_id AS rollup_id, r.metric_fmla_ln,
         s.ordinal, s.value AS trozo,
         LAG(s.value, 1, '') OVER (PARTITION BY r.src_file_nm, r.datapoint_id ORDER BY s.ordinal) AS trozo_anterior
  FROM r
  CROSS APPLY STRING_SPLIT(r.metric_fmla_ln, '[', 1) s
),
comp AS (
  SELECT src_file_nm, rollup_id, metric_fmla_ln, ordinal,
         TRIM(LEFT(trozo, CASE WHEN CHARINDEX(']', trozo) > 0 THEN CHARINDEX(']', trozo) - 1 ELSE 0 END)) AS componente,
         CASE WHEN (CASE WHEN CHARINDEX(']', trozo_anterior) > 0
                         THEN SUBSTRING(trozo_anterior, CHARINDEX(']', trozo_anterior) + 1, 8000)
                         ELSE trozo_anterior END) LIKE '%-%'
              THEN -1 ELSE 1 END AS signo
  FROM trozos
  WHERE CHARINDEX(']', trozo) > 0
)
SELECT c.rollup_id,
       c.metric_fmla_ln,
       c.signo,
       c.componente,
       CASE WHEN v.datapoint_id IS NULL THEN 'NO EXISTE EN EL FICHERO' ELSE v.datapoint_cl_typ END AS tipo_componente,
       v.var_cd,
       v.src_tbl_id,
       COALESCE(NULLIF(v.per_expr_cd, ''), CASE WHEN COALESCE(v.src_tbl_id, '') <> '' THEN 'M' END) AS periodo,
       v.metric_fmla_ln AS metrica_componente
FROM comp c
LEFT JOIN vig v ON v.src_file_nm = c.src_file_nm AND TRIM(v.datapoint_id) = c.componente
ORDER BY c.ordinal, v.var_cd;


-- 1.3b Rollups que usan el datapoint (impacto de cambiarlo) ---------------------
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp)
SELECT v.datapoint_id AS rollup_que_lo_usa,
       v.metric_fmla_ln,
       v.src_file_nm
FROM dbo.dp_lgl_reg_dpm_param_datapoint v
CROSS JOIN params p
WHERE v.ctrl_state = 1 AND v.row_present_flg = 1
  AND UPPER(v.framework_cd) = p.fw
  AND UPPER(v.datapoint_cl_typ) = 'ROLLUP'
  AND CHARINDEX(CONCAT('[', p.dp, ']'),
                REPLACE(REPLACE(v.metric_fmla_ln, '[ ', '['), ' ]', ']')) > 0
ORDER BY rollup_que_lo_usa;


-- 1.4 Historial de versiones del datapoint --------------------------------------
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp),
ficheros AS (
  SELECT DISTINCT d.src_file_nm
  FROM dbo.dp_lgl_reg_dpm_param_datapoint d
  JOIN params p ON UPPER(d.framework_cd) = p.fw
),
h AS (
  SELECT 'Datapoints' AS pestana, src_file_nm, src_row_nr, datapoint_id, var_cd,
         CONCAT_WS(' | ', datapoint_cl_typ, src_tbl_id, metric_fmla_ln, having_ln, per_expr_cd) AS contenido,
         ref_start_dt, ref_end_dt, ctrl_state, row_present_flg
  FROM dbo.dp_lgl_reg_dpm_param_datapoint
  UNION ALL
  SELECT 'Filters', src_file_nm, src_row_nr, datapoint_id, var_cd,
         CONCAT_WS(' | ', src_column_nm, optr_cd, fltr_val_ln),
         ref_start_dt, ref_end_dt, ctrl_state, row_present_flg
  FROM dbo.dp_lgl_reg_dpm_param_fltr
  UNION ALL
  SELECT 'Catalog', src_file_nm, src_row_nr, datapoint_id, CAST(NULL AS VARCHAR(50)),
         CONCAT_WS(' | ', tmplt_cd, row_cd, column_cd, val_typ),
         ref_start_dt, ref_end_dt, ctrl_state, row_present_flg
  FROM dbo.dp_lgl_reg_dpm_param_ctlg
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
  SELECT TOP 1 v.vld_id, UPPER(v.framework_cd) AS framework_cd, v.param_snpsh_dt,
         v.vld_stts, v.err_nr, v.crtn_dt
  FROM dbo.dp_lgl_reg_dpm_param_vld v
  CROSS JOIN params p
  WHERE UPPER(v.framework_cd) = p.fw AND (p.vld_id = '' OR v.vld_id = p.vld_id)
  ORDER BY v.crtn_dt DESC
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
LEFT JOIN dbo.dp_lgl_reg_dpm_iss_log i
       ON i.vld_id = u.vld_id
      AND UPPER(COALESCE(i.framework_cd, '')) IN (u.framework_cd, '')
GROUP BY u.vld_id, u.crtn_dt, u.param_snpsh_dt, u.vld_stts, u.err_nr, i.iss_cd
ORDER BY ocurrencias DESC, i.iss_cd;


-- 2.2 Detalle de incidencias con la parametría implicada --------------------------
-- La fila del Excel que provoca cada incidencia, en la instantánea validada.
WITH params AS (SELECT 'FCP' AS fw, '' AS dp, '' AS vld_id),
ult AS (
  SELECT TOP 1 v.vld_id, UPPER(v.framework_cd) AS framework_cd, v.param_snpsh_dt
  FROM dbo.dp_lgl_reg_dpm_param_vld v
  CROSS JOIN params p
  WHERE UPPER(v.framework_cd) = p.fw AND (p.vld_id = '' OR v.vld_id = p.vld_id)
  ORDER BY v.crtn_dt DESC
),
iss AS (
  SELECT i.*
  FROM dbo.dp_lgl_reg_dpm_iss_log i
  JOIN ult u ON i.vld_id = u.vld_id
  CROSS JOIN params p
  WHERE UPPER(COALESCE(i.framework_cd, '')) IN (u.framework_cd, '')
    AND (p.dp = '' OR TRIM(i.datapoint_id) = p.dp)
),
snap_dp AS (
  SELECT d.*
  FROM dbo.dp_lgl_reg_dpm_param_datapoint d
  JOIN ult u ON UPPER(d.framework_cd) = u.framework_cd
  WHERE d.ref_start_dt <= u.param_snpsh_dt
    AND (d.ref_end_dt IS NULL OR d.ref_end_dt > u.param_snpsh_dt)
    AND d.row_present_flg = 1
),
snap_f AS (
  SELECT f.src_file_nm, f.datapoint_id, UPPER(COALESCE(f.var_cd, '')) AS var_cd,
         STRING_AGG(CAST(CONCAT(f.src_column_nm, ' ', UPPER(f.optr_cd), ' ', f.fltr_val_ln) AS VARCHAR(MAX)), '  AND  ')
           WITHIN GROUP (ORDER BY f.src_column_nm, f.fltr_val_ln) AS where_legible
  FROM dbo.dp_lgl_reg_dpm_param_fltr f
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
WITH params AS (SELECT 'FCP' AS fw),
v AS (
  SELECT v.vld_id, v.crtn_dt, ROW_NUMBER() OVER (ORDER BY v.crtn_dt DESC) AS rn
  FROM dbo.dp_lgl_reg_dpm_param_vld v
  JOIN params p ON UPPER(v.framework_cd) = p.fw
),
i AS (
  SELECT DISTINCT v.rn, i.iss_cd, i.datapoint_id, i.var_id, i.msg_ln
  FROM dbo.dp_lgl_reg_dpm_iss_log i
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
  AND (u.datapoint_id = pu.datapoint_id OR (u.datapoint_id IS NULL AND pu.datapoint_id IS NULL))
  AND (u.var_id       = pu.var_id       OR (u.var_id IS NULL       AND pu.var_id IS NULL))
  AND (u.msg_ln       = pu.msg_ln       OR (u.msg_ln IS NULL       AND pu.msg_ln IS NULL))
ORDER BY situacion, iss_cd, datapoint_id;


-- 2.4 Historial de incidencias de validación de un datapoint ---------------------
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp)
SELECT v.crtn_dt AS validado_dt,
       v.vld_id,
       v.vld_stts AS veredicto_framework,
       i.iss_cd,
       NULLIF(i.var_id, '') AS variable,
       i.msg_ln
FROM dbo.dp_lgl_reg_dpm_iss_log i
JOIN dbo.dp_lgl_reg_dpm_param_vld v ON v.vld_id = i.vld_id
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
  SELECT TOP 1 e.*
  FROM dbo.dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND CONCAT(',', UPPER(e.framework_list_ln), ',') LIKE CONCAT('%,', p.fw, ',%')
         AND (p.periodo = '' OR CONVERT(CHAR(6), e.ref_dt, 112) = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
)
SELECT e.exec_id,
       CONVERT(CHAR(6), e.ref_dt, 112) AS periodo,
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
LEFT JOIN dbo.dp_lgl_reg_dpm_iss_log i ON i.exec_id = e.exec_id
GROUP BY e.exec_id, e.ref_dt, e.exec_stts, e.param_snpsh_dt, e.start_dt, e.end_dt, e.msg_ln,
         i.sev_cd, i.iss_cd
ORDER BY i.sev_cd, ocurrencias DESC;


-- 3.2 Detalle de incidencias de la ejecución -----------------------------------------
WITH params AS (SELECT 'FCP' AS fw, '' AS periodo, '' AS exec_id, '' AS dp),
ejec AS (
  SELECT TOP 1 e.*
  FROM dbo.dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND CONCAT(',', UPPER(e.framework_list_ln), ',') LIKE CONCAT('%,', p.fw, ',%')
         AND (p.periodo = '' OR CONVERT(CHAR(6), e.ref_dt, 112) = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
)
SELECT i.sev_cd,
       i.iss_cd,
       NULLIF(i.datapoint_id, '')        AS datapoint_id,
       COALESCE(lv.var_cd,
                CASE WHEN i.var_id LIKE 'CALC[_]%' THEN 'RESULT' END,
                NULLIF(i.var_id, ''))    AS variable,
       NULLIF(i.src_tbl_id, '')          AS tabla,
       i.afct_nr,
       i.msg_ln,
       NULLIF(i.grp_id, '')              AS grupo,
       NULLIF(i.raw_err_cla_nm, '')      AS error_tecnico,
       NULLIF(i.raw_msg_ln, '')          AS detalle_tecnico,
       i.exec_id
FROM dbo.dp_lgl_reg_dpm_iss_log i
JOIN ejec e ON i.exec_id = e.exec_id
LEFT JOIN dbo.dp_lgl_reg_dpm_lineage_var lv
       ON lv.exec_id = i.exec_id AND lv.var_id = i.var_id
CROSS JOIN params p
WHERE p.dp = '' OR TRIM(i.datapoint_id) = p.dp
ORDER BY i.sev_cd, i.iss_cd, datapoint_id, variable;


-- 3.3 Pasos y tiempos de la ejecución ---------------------------------------------
WITH params AS (SELECT 'FCP' AS fw, '' AS periodo, '' AS exec_id),
ejec AS (
  SELECT TOP 1 e.*
  FROM dbo.dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND CONCAT(',', UPPER(e.framework_list_ln), ',') LIKE CONCAT('%,', p.fw, ',%')
         AND (p.periodo = '' OR CONVERT(CHAR(6), e.ref_dt, 112) = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
)
SELECT s.step_nm,
       NULLIF(s.grp_id, '')                       AS grupo,
       s.step_stts,
       s.row_nr,
       DATEDIFF(SECOND, s.start_dt, s.end_dt)     AS segundos,
       s.msg_ln,
       s.start_dt,
       s.end_dt
FROM dbo.dp_lgl_reg_dpm_exec_step s
JOIN ejec e ON s.exec_id = e.exec_id
ORDER BY segundos DESC;


/* =============================================================================
   4. UN DATAPOINT EN UNA EJECUCIÓN
   ============================================================================= */

-- 4.1 Cabecera del datapoint ---------------------------------------------------------
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp, '' AS periodo, '' AS exec_id),
ejec AS (
  SELECT TOP 1 e.*
  FROM dbo.dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND CONCAT(',', UPPER(e.framework_list_ln), ',') LIKE CONCAT('%,', p.fw, ',%')
         AND (p.periodo = '' OR CONVERT(CHAR(6), e.ref_dt, 112) = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
),
ld AS (
  SELECT l.*
  FROM dbo.dp_lgl_reg_dpm_lineage_datapoint l
  JOIN ejec e ON l.exec_id = e.exec_id
  CROSS JOIN params p
  WHERE TRIM(l.datapoint_id) = p.dp
),
res AS (
  SELECT COUNT(*) AS filas_calculadas, SUM(r.contributing_row_nr) AS registros_origen
  FROM dbo.dp_lgl_reg_dpm_result_datapoint_val r
  JOIN ejec e ON r.exec_id = e.exec_id
  CROSS JOIN params p
  WHERE TRIM(r.datapoint_id) = p.dp
),
pub AS (
  SELECT COUNT(*) AS filas_publicadas
  FROM dbo.dp_lgl_reg_dpm_rpt_datapoint r
  JOIN ejec e ON r.exec_id = e.exec_id
  CROSS JOIN params p
  WHERE TRIM(r.datapoint_id) = p.dp
),
iss AS (
  SELECT STRING_AGG(CAST(x.incidencia AS VARCHAR(MAX)), ', ') AS incidencias
  FROM (
    SELECT DISTINCT CONCAT(i.sev_cd, ' ', i.iss_cd) AS incidencia
    FROM dbo.dp_lgl_reg_dpm_iss_log i
    JOIN ejec e ON i.exec_id = e.exec_id
    CROSS JOIN params p
    WHERE TRIM(i.datapoint_id) = p.dp
  ) x
)
SELECT e.exec_id,
       CONVERT(CHAR(6), e.ref_dt, 112) AS periodo,
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
       ld.datapoint_sql_ln             -- Spark SQL: ejecutarlo en el notebook
FROM ejec e
CROSS JOIN params p
LEFT JOIN ld ON 1 = 1
CROSS JOIN res
CROSS JOIN pub
CROSS JOIN iss;


-- 4.2 Variables del datapoint: SELECT ejecutado, importe e issue code -------------
-- src_sql_ln        : SELECT original del motor (Spark SQL)
-- sql_tsql          : el mismo SELECT traducido a T-SQL, ejecutable aquí
-- sql_detalle_tsql  : sin agregar, para ver los registros de origen (sin HAVING)
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp, '' AS periodo, '' AS exec_id),
ejec AS (
  SELECT TOP 1 e.*
  FROM dbo.dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND CONCAT(',', UPPER(e.framework_list_ln), ',') LIKE CONCAT('%,', p.fw, ',%')
         AND (p.periodo = '' OR CONVERT(CHAR(6), e.ref_dt, 112) = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
),
vars AS (
  SELECT l.exec_id, l.datapoint_id AS datapoint_pedido,
         j.datapoint_id, j.var_cd, j.var_id, j.sign
  FROM dbo.dp_lgl_reg_dpm_lineage_datapoint l
  JOIN ejec e ON l.exec_id = e.exec_id
  CROSS JOIN params p
  CROSS APPLY OPENJSON(l.var_map_json_ln)
       WITH (datapoint_id VARCHAR(200) '$.datapoint_id',
             var_cd       VARCHAR(50)  '$.var_cd',
             var_id       VARCHAR(200) '$.var_id',
             sign         INT          '$.sign') j
  WHERE TRIM(l.datapoint_id) = p.dp
),
val AS (
  SELECT r.var_id,
         SUM(r.val_amnt)            AS importe_total_claves,
         COUNT(*)                   AS claves_con_valor,
         SUM(r.contributing_row_nr) AS registros_origen
  FROM dbo.dp_lgl_reg_dpm_result_var_val r
  JOIN ejec e ON r.exec_id = e.exec_id
  WHERE r.var_id IN (SELECT var_id FROM vars)
  GROUP BY r.var_id
),
fk AS (
  SELECT x.var_id, SUM(x.n) AS claves_fallidas,
         STRING_AGG(CAST(x.iss_cd AS VARCHAR(MAX)), ', ') AS motivo_claves_fallidas
  FROM (
    SELECT f.var_id, f.iss_cd, COUNT(*) AS n
    FROM dbo.dp_lgl_reg_dpm_failed_var_key f
    JOIN ejec e ON f.exec_id = e.exec_id
    WHERE f.var_id IN (SELECT var_id FROM vars)
    GROUP BY f.var_id, f.iss_cd
  ) x
  GROUP BY x.var_id
),
iss AS (
  SELECT x.var_id,
         STRING_AGG(CAST(x.iss_cd AS VARCHAR(MAX)), ', ') AS issue_cd,
         MIN(x.msg_ln)                                    AS issue_msg
  FROM (
    SELECT i.var_id, i.iss_cd, MIN(i.msg_ln) AS msg_ln
    FROM dbo.dp_lgl_reg_dpm_iss_log i
    JOIN ejec e ON i.exec_id = e.exec_id
    WHERE i.var_id IN (SELECT var_id FROM vars)
    GROUP BY i.var_id, i.iss_cd
  ) x
  GROUP BY x.var_id
),
sqls AS (
  -- Traducción Spark SQL -> T-SQL de la sintaxis que genera el motor.
  SELECT lv.exec_id, lv.var_id, lv.src_tbl_id, lv.agg_fcn_cd, lv.metric_fmla_ln,
         lv.having_ln, lv.per_cd, lv.src_sql_ln,
         REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(lv.src_sql_ln,
             '`', '"'),
             ' AS STRING)', ' AS VARCHAR(8000))'),
             'DATE''', ''''),
             'FALSE /*', '1 = 0 /*'),
             'FROM DP_FET_LH_LegalAndRegulatoryReporting.', 'FROM DP_FET_LH_LegalAndRegulatoryReporting.dbo.')
           AS sql_tsql
  FROM dbo.dp_lgl_reg_dpm_lineage_var lv
  JOIN ejec e ON lv.exec_id = e.exec_id
  WHERE lv.var_id IN (SELECT var_id FROM vars)
)
SELECT v.datapoint_pedido,
       v.datapoint_id          AS datapoint_de_la_variable,
       v.var_cd,
       v.sign                  AS signo,
       s.src_tbl_id,
       s.agg_fcn_cd,
       s.metric_fmla_ln,
       s.having_ln,
       s.per_cd                AS periodo_leido,
       val.importe_total_claves,
       val.claves_con_valor,
       val.registros_origen,
       fk.claves_fallidas,
       fk.motivo_claves_fallidas,
       iss.issue_cd,
       iss.issue_msg,
       s.sql_tsql,
       CONCAT('SELECT *', CHAR(10),
              CASE WHEN c.p_gb > 0 THEN LEFT(b.resto, c.p_gb - 1) ELSE b.resto END) AS sql_detalle_tsql,
       s.src_sql_ln,
       v.var_id
FROM vars v
LEFT JOIN sqls s ON s.exec_id = v.exec_id AND s.var_id = v.var_id
LEFT JOIN val ON val.var_id = v.var_id
LEFT JOIN fk  ON fk.var_id  = v.var_id
LEFT JOIN iss ON iss.var_id = v.var_id
CROSS APPLY (SELECT CHARINDEX(CONCAT(CHAR(10), 'FROM '), s.sql_tsql) AS p_from) a
CROSS APPLY (SELECT CASE WHEN a.p_from > 0
                         THEN SUBSTRING(s.sql_tsql, a.p_from + 1, LEN(s.sql_tsql)) END AS resto) b
CROSS APPLY (SELECT CHARINDEX(CONCAT(CHAR(10), 'GROUP BY '), b.resto) AS p_gb) c
ORDER BY v.datapoint_id, v.var_cd;


-- 4.3 Importes del datapoint por vista y entidad, con el valor publicado ----------
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp, '' AS periodo, '' AS exec_id),
ejec AS (
  SELECT TOP 1 e.*
  FROM dbo.dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND CONCAT(',', UPPER(e.framework_list_ln), ',') LIKE CONCAT('%,', p.fw, ',%')
         AND (p.periodo = '' OR CONVERT(CHAR(6), e.ref_dt, 112) = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
),
calc AS (
  SELECT r.exec_id, r.datapoint_id, r.csl_view_cd, r.csl_perim_typ, r.csl_ent_cd,
         r.datapoint_typ, r.ctry_cd, r.val_amnt, CAST(NULL AS VARCHAR(8000)) AS valor_default,
         r.contributing_row_nr, r.key_json_ln
  FROM dbo.dp_lgl_reg_dpm_result_datapoint_val r
  JOIN ejec e ON r.exec_id = e.exec_id
  CROSS JOIN params p
  WHERE TRIM(r.datapoint_id) = p.dp
  UNION ALL
  SELECT d.exec_id, d.datapoint_id, d.csl_view_cd, d.csl_perim_typ, d.csl_ent_cd,
         d.datapoint_typ, d.ctry_cd, CAST(NULL AS DECIMAL(38,10)), d.def_val_ln,
         CAST(NULL AS BIGINT), CAST(NULL AS VARCHAR(8000))
  FROM dbo.dp_lgl_reg_dpm_datapoint_def d
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
LEFT JOIN dbo.dp_lgl_reg_dpm_rpt_datapoint r
       ON r.exec_id = c.exec_id AND r.datapoint_id = c.datapoint_id
      AND (r.csl_view_cd   = c.csl_view_cd   OR (r.csl_view_cd   IS NULL AND c.csl_view_cd   IS NULL))
      AND (r.csl_perim_typ = c.csl_perim_typ OR (r.csl_perim_typ IS NULL AND c.csl_perim_typ IS NULL))
      AND (r.csl_ent_cd    = c.csl_ent_cd    OR (r.csl_ent_cd    IS NULL AND c.csl_ent_cd    IS NULL))
      AND (r.datapoint_typ = c.datapoint_typ OR (r.datapoint_typ IS NULL AND c.datapoint_typ IS NULL))
      AND (r.ctry_cd       = c.ctry_cd       OR (r.ctry_cd       IS NULL AND c.ctry_cd       IS NULL))
ORDER BY c.csl_view_cd, c.csl_ent_cd, c.datapoint_typ, c.ctry_cd;


-- 4.4 Importe de cada variable por clave (entidad) y claves fallidas --------------
WITH params AS (SELECT 'FCP' AS fw, 'T02.00,i0240' AS dp, '' AS periodo, '' AS exec_id),
ejec AS (
  SELECT TOP 1 e.*
  FROM dbo.dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE (p.exec_id <> '' AND e.exec_id = p.exec_id)
     OR (p.exec_id = '' AND CONCAT(',', UPPER(e.framework_list_ln), ',') LIKE CONCAT('%,', p.fw, ',%')
         AND (p.periodo = '' OR CONVERT(CHAR(6), e.ref_dt, 112) = p.periodo)
         AND e.exec_stts <> 'RUNNING')
  ORDER BY e.start_dt DESC
),
var_ids AS (
  SELECT j.var_id
  FROM dbo.dp_lgl_reg_dpm_lineage_datapoint l
  JOIN ejec e ON l.exec_id = e.exec_id
  CROSS JOIN params p
  CROSS APPLY OPENJSON(l.var_map_json_ln) WITH (var_id VARCHAR(200) '$.var_id') j
  WHERE TRIM(l.datapoint_id) = p.dp
)
SELECT r.datapoint_id,
       r.var_cd,
       r.var_cl_typ,
       JSON_VALUE(r.key_json_ln, '$.ENTIDADE') AS entidad,
       JSON_VALUE(r.key_json_ln, '$.IDENT')    AS ident,
       JSON_VALUE(r.key_json_ln, '$.IDSUBENT') AS subentidad,
       JSON_VALUE(r.key_json_ln, '$.CDWA')     AS pais,
       r.val_amnt,
       r.contributing_row_nr AS registros_origen,
       'OK'                  AS estado,
       r.key_hsh_cd
FROM dbo.dp_lgl_reg_dpm_result_var_val r
JOIN ejec e ON r.exec_id = e.exec_id
CROSS JOIN params p
WHERE r.var_id IN (SELECT var_id FROM var_ids)
   OR (TRIM(r.datapoint_id) = p.dp AND r.var_cl_typ = 'DERIVED')
UNION ALL
SELECT f.datapoint_id,
       COALESCE(lv.var_cd, CASE WHEN f.var_id LIKE 'CALC[_]%' THEN 'RESULT' END),
       CASE WHEN f.var_id LIKE 'CALC[_]%' THEN 'DERIVED' ELSE 'BASE' END,
       NULL, NULL, NULL, NULL,
       CAST(NULL AS DECIMAL(38,10)),
       CAST(NULL AS BIGINT),
       CONCAT('FALLIDA: ', f.iss_cd),
       f.key_hsh_cd
FROM dbo.dp_lgl_reg_dpm_failed_var_key f
JOIN ejec e ON f.exec_id = e.exec_id
LEFT JOIN dbo.dp_lgl_reg_dpm_lineage_var lv ON lv.exec_id = f.exec_id AND lv.var_id = f.var_id
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
  FROM dbo.dp_lgl_reg_dpm_param_datapoint d
  JOIN params p ON UPPER(d.framework_cd) = p.fw
),
filas AS (
  SELECT 'Datapoints' AS pestana, src_file_nm, datapoint_id AS objeto, var_cd,
         CAST(NULL AS VARCHAR(8000)) AS columna,
         CONCAT_WS(' | ', datapoint_cl_typ, src_tbl_id, metric_fmla_ln, having_ln, per_expr_cd) AS contenido,
         src_row_nr, ref_start_dt, ref_end_dt, row_present_flg
  FROM dbo.dp_lgl_reg_dpm_param_datapoint
  UNION ALL
  SELECT 'Filters', src_file_nm, datapoint_id, var_cd, UPPER(src_column_nm),
         CONCAT_WS(' | ', src_column_nm, optr_cd, fltr_val_ln),
         src_row_nr, ref_start_dt, ref_end_dt, row_present_flg
  FROM dbo.dp_lgl_reg_dpm_param_fltr
  UNION ALL
  SELECT 'Catalog', src_file_nm, datapoint_id, CAST(NULL AS VARCHAR(8000)), CAST(NULL AS VARCHAR(8000)),
         CONCAT_WS(' | ', tmplt_cd, row_cd, column_cd, val_typ),
         src_row_nr, ref_start_dt, ref_end_dt, row_present_flg
  FROM dbo.dp_lgl_reg_dpm_param_ctlg
  UNION ALL
  SELECT 'Sources', src_file_nm, src_tbl_id, CAST(NULL AS VARCHAR(8000)), CAST(NULL AS VARCHAR(8000)),
         CONCAT_WS(' | ', phy_tbl_nm, per_column_nm, per_fmt_cd, ent_column_nm,
                          ident_column_nm, subent_column_nm, ctry_column_nm, grain_id),
         src_row_nr, ref_start_dt, ref_end_dt, row_present_flg
  FROM dbo.dp_lgl_reg_dpm_param_src_tbl
),
fw_filas AS (
  SELECT f.* FROM filas f JOIN ficheros fi ON fi.src_file_nm = f.src_file_nm
),
ts AS (SELECT CASE WHEN MAX(ref_end_dt) > MAX(ref_start_dt) THEN MAX(ref_end_dt) ELSE MAX(ref_start_dt) END AS ts FROM fw_filas),
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
  AND (c.objeto  = n.objeto  OR (c.objeto  IS NULL AND n.objeto  IS NULL))
  AND (c.var_cd  = n.var_cd  OR (c.var_cd  IS NULL AND n.var_cd  IS NULL))
  AND (c.columna = n.columna OR (c.columna IS NULL AND n.columna IS NULL))
WHERE n.pestana IS NULL OR c.pestana IS NULL
   OR NOT (n.contenido = c.contenido OR (n.contenido IS NULL AND c.contenido IS NULL))
ORDER BY pestana, objeto, var_cd;


-- 5.2 Datapoints del catálogo no publicados en un periodo y su motivo -----------------
WITH params AS (SELECT 'FCP' AS fw, '202512' AS periodo),
ejec AS (
  SELECT TOP 1 e.*
  FROM dbo.dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE CONCAT(',', UPPER(e.framework_list_ln), ',') LIKE CONCAT('%,', p.fw, ',%')
    AND CONVERT(CHAR(6), e.ref_dt, 112) = p.periodo
  ORDER BY e.start_dt DESC
),
cat AS (
  SELECT c.src_file_nm, c.datapoint_id, c.tmplt_cd, c.row_cd, c.column_cd
  FROM dbo.dp_lgl_reg_dpm_param_ctlg c
  JOIN params p ON UPPER(c.framework_cd) = p.fw
  WHERE c.ctrl_state = 1 AND c.row_present_flg = 1
),
pub AS (
  SELECT DISTINCT r.datapoint_id
  FROM dbo.dp_lgl_reg_dpm_rpt_datapoint r
  CROSS JOIN params p
  WHERE UPPER(r.framework_cd) = p.fw AND CONVERT(CHAR(6), r.ref_dt, 112) = p.periodo
),
dp_tabla AS (
  SELECT DISTINCT d.src_file_nm, d.datapoint_id, d.datapoint_cl_typ, s.phy_tbl_nm
  FROM dbo.dp_lgl_reg_dpm_param_datapoint d
  LEFT JOIN dbo.dp_lgl_reg_dpm_param_src_tbl s
         ON s.src_file_nm = d.src_file_nm AND UPPER(s.src_tbl_id) = UPPER(d.src_tbl_id)
        AND s.ctrl_state = 1 AND s.row_present_flg = 1
  WHERE d.ctrl_state = 1 AND d.row_present_flg = 1
),
tipo AS (
  SELECT x.src_file_nm, x.datapoint_id,
         STRING_AGG(CAST(x.datapoint_cl_typ AS VARCHAR(MAX)), ', ') AS tipo
  FROM (SELECT DISTINCT src_file_nm, datapoint_id, datapoint_cl_typ FROM dp_tabla) x
  GROUP BY x.src_file_nm, x.datapoint_id
),
iss_dp AS (
  SELECT x.datapoint_id, STRING_AGG(CAST(x.iss_cd AS VARCHAR(MAX)), ', ') AS incidencias
  FROM (
    SELECT DISTINCT i.datapoint_id, i.iss_cd
    FROM dbo.dp_lgl_reg_dpm_iss_log i JOIN ejec e ON i.exec_id = e.exec_id
  ) x
  GROUP BY x.datapoint_id
),
dp_sin_periodo AS (
  SELECT DISTINCT t.src_file_nm, t.datapoint_id
  FROM dp_tabla t
  JOIN dbo.dp_lgl_reg_dpm_iss_log i ON UPPER(i.src_tbl_id) = UPPER(t.phy_tbl_nm)
  JOIN ejec e ON i.exec_id = e.exec_id
  WHERE i.iss_cd = 'CALC.INPUT.NO_PERIOD_ROWS'
)
SELECT c.datapoint_id,
       c.tmplt_cd, c.row_cd, c.column_cd,
       t.tipo,
       (SELECT MAX(exec_id)   FROM ejec) AS ultima_ejecucion_periodo,
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
WITH params AS (SELECT 'FCP' AS fw, '202512' AS periodo,
                       '' AS exec_anterior, '' AS exec_nueva, 0.01 AS umbral),
ej AS (
  SELECT e.exec_id, ROW_NUMBER() OVER (ORDER BY e.start_dt DESC) AS rn
  FROM dbo.dp_lgl_reg_dpm_exec e
  CROSS JOIN params p
  WHERE CONCAT(',', UPPER(e.framework_list_ln), ',') LIKE CONCAT('%,', p.fw, ',%')
    AND CONVERT(CHAR(6), e.ref_dt, 112) = p.periodo
    AND e.exec_stts NOT IN ('FAILED', 'RUNNING')
),
sel AS (
  SELECT COALESCE(NULLIF(p.exec_anterior, ''), MAX(CASE WHEN ej.rn = 2 THEN ej.exec_id END)) AS a,
         COALESCE(NULLIF(p.exec_nueva, ''),    MAX(CASE WHEN ej.rn = 1 THEN ej.exec_id END)) AS b
  FROM params p LEFT JOIN ej ON 1 = 1
  GROUP BY p.exec_anterior, p.exec_nueva
),
a AS (SELECT r.* FROM dbo.dp_lgl_reg_dpm_result_datapoint_val r JOIN sel ON r.exec_id = sel.a),
b AS (SELECT r.* FROM dbo.dp_lgl_reg_dpm_result_datapoint_val r JOIN sel ON r.exec_id = sel.b)
SELECT COALESCE(a.datapoint_id, b.datapoint_id)   AS datapoint_id,
       COALESCE(a.csl_view_cd,  b.csl_view_cd)    AS csl_view_cd,
       COALESCE(a.csl_ent_cd,   b.csl_ent_cd)     AS csl_ent_cd,
       COALESCE(a.datapoint_typ, b.datapoint_typ) AS datapoint_typ,
       COALESCE(a.ctry_cd,      b.ctry_cd)        AS ctry_cd,
       a.val_amnt                                 AS valor_anterior,
       b.val_amnt                                 AS valor_nuevo,
       COALESCE(b.val_amnt, 0) - COALESCE(a.val_amnt, 0) AS diferencia,
       CASE WHEN a.datapoint_id IS NULL THEN 'SOLO EN LA NUEVA'
            WHEN b.datapoint_id IS NULL THEN 'SOLO EN LA ANTERIOR'
            ELSE 'DISTINTO' END                   AS situacion,
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
WITH params AS (SELECT '' AS fw, '' AS tabla, 'NATACCOUNT' AS columna, '1001' AS valor),
d AS (
  SELECT * FROM dbo.dp_lgl_reg_dpm_param_datapoint
  WHERE ctrl_state = 1 AND row_present_flg = 1
),
f AS (
  SELECT * FROM dbo.dp_lgl_reg_dpm_param_fltr
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
                      OR UPPER(d.metric_fmla_ln) LIKE CONCAT('%', UPPER(p.columna), '%'))
  AND (p.valor = ''   OR UPPER(f.fltr_val_ln) LIKE CONCAT('%', UPPER(p.valor), '%'))
ORDER BY d.framework_cd, d.datapoint_id, variable;


-- 5.5 Ejecuciones colgadas en RUNNING --------------------------------------------------
WITH params AS (SELECT 30 AS minutos_sin_avance),
ult_paso AS (
  SELECT exec_id, step_nm, end_dt,
         ROW_NUMBER() OVER (PARTITION BY exec_id ORDER BY end_dt DESC) AS rn
  FROM dbo.dp_lgl_reg_dpm_exec_step
)
SELECT e.exec_id,
       e.framework_list_ln,
       CONVERT(CHAR(6), e.ref_dt, 112) AS periodo,
       e.start_dt,
       s.end_dt                        AS ultimo_paso_dt,
       s.step_nm                       AS ultimo_paso
FROM dbo.dp_lgl_reg_dpm_exec e
LEFT JOIN ult_paso s ON s.exec_id = e.exec_id AND s.rn = 1
CROSS JOIN params p
WHERE e.exec_stts = 'RUNNING'
  AND COALESCE(s.end_dt, e.start_dt) < DATEADD(MINUTE, -p.minutos_sin_avance, SYSUTCDATETIME())
ORDER BY e.start_dt;


-- 5.6 Significado de un código de incidencia --------------------------------------------
WITH params AS (SELECT 'COLUMN_NOT_FOUND' AS codigo)
SELECT c.iss_cd, c.dmn_cd, c.stg_cd, c.sev_cd, c.scp_cd, c.act_flg,
       c.msg_tmplt_ln, c.param_nm_list_ln
FROM dbo.dp_lgl_reg_dpm_param_iss_ctlg c
CROSS JOIN params p
WHERE c.ctrl_state = 1 AND c.row_present_flg = 1
  AND UPPER(c.iss_cd) LIKE CONCAT('%', UPPER(p.codigo), '%')
ORDER BY c.iss_cd;


-- 5.7 Incidencias más frecuentes de los últimos 30 días -------------------------------
WITH params AS (SELECT '' AS fw, 30 AS dias)
SELECT i.phs_cd,
       UPPER(i.framework_cd)          AS framework_cd,
       i.sev_cd,
       i.iss_cd,
       COUNT(*)                       AS ocurrencias,
       COUNT(DISTINCT COALESCE(NULLIF(i.vld_id, ''), i.exec_id)) AS procesos,
       COUNT(DISTINCT NULLIF(i.datapoint_id, '')) AS datapoints,
       MAX(i.crtn_dt)                 AS ultima_vez
FROM dbo.dp_lgl_reg_dpm_iss_log i
CROSS JOIN params p
WHERE i.crtn_dt >= DATEADD(DAY, -p.dias, SYSUTCDATETIME())
  AND (p.fw = '' OR UPPER(i.framework_cd) = p.fw)
GROUP BY i.phs_cd, UPPER(i.framework_cd), i.sev_cd, i.iss_cd
ORDER BY ocurrencias DESC;


-- 5.8 Últimas subidas de parametría (DataLoader) ---------------------------------------
-- Una fila por subida: fichero, quién la subió, estado de la promoción y veredicto
-- de su validación. fw = '' para ver todos los frameworks.
WITH params AS (SELECT 'FCP' AS fw)
SELECT TOP 20 l.crtn_dt,
       l.framework_cd,
       l.src_file_nm,
       l.load_usr_id,
       l.load_sts_cd,
       v.vld_stts,
       v.err_nr,
       l.param_snpsh_dt,
       l.vld_id,
       l.datapoint_load_id, l.fltr_load_id, l.ctlg_load_id, l.src_load_id
FROM dbo.dp_lgl_reg_dpm_param_load l
LEFT JOIN dbo.dp_lgl_reg_dpm_param_vld v
       ON v.vld_id = l.vld_id AND UPPER(v.framework_cd) = UPPER(l.framework_cd)
CROSS JOIN params p
WHERE p.fw = '' OR UPPER(l.framework_cd) = p.fw
ORDER BY l.crtn_dt DESC;
