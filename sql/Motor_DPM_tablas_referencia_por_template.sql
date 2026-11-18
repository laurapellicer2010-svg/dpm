-- =============================================================================
-- Motor DPM · Tablas de referencia por template: clave (framework_cd, tmplt_cd)
-- El template se escribe igual que en la pestaña Catalog (sin la letra del framework).
-- Valores: perímetros = FRP_TPMAPA_MAPEAMENTO de legacy (65 filas, 18/11/2026);
-- país = FRP_ZAXIX_MAPA_COM_PAIS (valores de arranque del notebook de migración);
-- tipos = los de FET (dos de legacy + las dos tablas nuevas de FINREP).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- PARTE A · Solo en FET (una vez): convertir las tablas actuales al formato nuevo.
-- Ejecutar a la vez que se despliega el Processing nuevo: el actual deja de
-- encontrar el perímetro y el país en cuanto las tablas cambian.
-- -----------------------------------------------------------------------------

-- A.0 Comprobación previa: tiene que devolver 0 filas (todas empiezan por 'F ').
SELECT 'ref_tmplt_csl_perim' AS tabla, tmplt_cd
FROM dp_lgl_reg_dpm_ref_tmplt_csl_perim
WHERE TRIM(tmplt_cd) NOT LIKE 'F %'
UNION ALL
SELECT 'ref_tmplt_ctry' AS tabla, tmplt_cd
FROM dp_lgl_reg_dpm_ref_tmplt_ctry
WHERE TRIM(tmplt_cd) NOT LIKE 'F %';

-- A.1 Copias de seguridad.
CREATE TABLE dp_lgl_reg_dpm_ref_tmplt_csl_perim_bkp_20261118 AS
SELECT tmplt_cd, csl_perim_typ FROM dp_lgl_reg_dpm_ref_tmplt_csl_perim;

CREATE TABLE dp_lgl_reg_dpm_ref_tmplt_ctry_bkp_20261118 AS
SELECT tmplt_cd, ctry_flg FROM dp_lgl_reg_dpm_ref_tmplt_ctry;

-- A.2 Reconstrucción con framework_cd y el template sin la letra.
CREATE OR REPLACE TABLE dp_lgl_reg_dpm_ref_tmplt_csl_perim AS
SELECT
    'FINREP' AS framework_cd
    , TRIM(SUBSTRING(TRIM(tmplt_cd), 3)) AS tmplt_cd
    , csl_perim_typ
FROM dp_lgl_reg_dpm_ref_tmplt_csl_perim_bkp_20261118;

CREATE OR REPLACE TABLE dp_lgl_reg_dpm_ref_tmplt_ctry AS
SELECT
    'FINREP' AS framework_cd
    , TRIM(SUBSTRING(TRIM(tmplt_cd), 3)) AS tmplt_cd
    , ctry_flg
FROM dp_lgl_reg_dpm_ref_tmplt_ctry_bkp_20261118;

-- A.3 Comprobación: mismo número de filas que la copia y ninguna clave repetida.
SELECT
    'ref_tmplt_csl_perim' AS tabla
    , COUNT(*) AS filas
    , COUNT(DISTINCT framework_cd, tmplt_cd) AS claves
FROM dp_lgl_reg_dpm_ref_tmplt_csl_perim
UNION ALL
SELECT
    'ref_tmplt_ctry' AS tabla
    , COUNT(*) AS filas
    , COUNT(DISTINCT framework_cd, tmplt_cd) AS claves
FROM dp_lgl_reg_dpm_ref_tmplt_ctry;


-- -----------------------------------------------------------------------------
-- PARTE B · Despliegue: CREATE (notebook de creación de tablas)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dp_lgl_reg_dpm_ref_tmplt_csl_perim (
    framework_cd STRING
    , tmplt_cd STRING
    , csl_perim_typ STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS dp_lgl_reg_dpm_ref_tmplt_ctry (
    framework_cd STRING
    , tmplt_cd STRING
    , ctry_flg INT
) USING DELTA;

CREATE TABLE IF NOT EXISTS dp_lgl_reg_dpm_ref_src_datapoint_typ (
    src_tbl_id STRING
    , datapoint_typ STRING
) USING DELTA;


-- -----------------------------------------------------------------------------
-- PARTE C · Despliegue: datos (notebook de inserts). MERGE: se puede relanzar
-- sin duplicar filas.
-- -----------------------------------------------------------------------------

MERGE INTO dp_lgl_reg_dpm_ref_tmplt_csl_perim AS t
USING (
    SELECT
        framework_cd
        , tmplt_cd
        , csl_perim_typ
    FROM VALUES
    ('FINREP', '01.01', 'PRUDENCIAL')
    , ('FINREP', '01.02', 'PRUDENCIAL')
    , ('FINREP', '01.03', 'PRUDENCIAL')
    , ('FINREP', '02.00', 'PRUDENCIAL')
    , ('FINREP', '03.00', 'PRUDENCIAL')
    , ('FINREP', '04.01', 'PRUDENCIAL')
    , ('FINREP', '04.02', 'PRUDENCIAL')
    , ('FINREP', '04.03', 'PRUDENCIAL')
    , ('FINREP', '04.04', 'PRUDENCIAL')
    , ('FINREP', '04.05', 'PRUDENCIAL')
    , ('FINREP', '05.00', 'PRUDENCIAL')
    , ('FINREP', '06.00', 'PRUDENCIAL')
    , ('FINREP', '07.00', 'PRUDENCIAL')
    , ('FINREP', '08.01', 'PRUDENCIAL')
    , ('FINREP', '08.02', 'PRUDENCIAL')
    , ('FINREP', '09.01', 'PRUDENCIAL')
    , ('FINREP', '09.02', 'PRUDENCIAL')
    , ('FINREP', '10.00', 'PRUDENCIAL')
    , ('FINREP', '11.01', 'PRUDENCIAL')
    , ('FINREP', '12.00', 'PRUDENCIAL')
    , ('FINREP', '13.01', 'PRUDENCIAL')
    , ('FINREP', '13.02', 'PRUDENCIAL')
    , ('FINREP', '13.03', 'PRUDENCIAL')
    , ('FINREP', '14.00', 'PRUDENCIAL')
    , ('FINREP', '15.00', 'PRUDENCIAL')
    , ('FINREP', '16.01', 'PRUDENCIAL')
    , ('FINREP', '16.02', 'PRUDENCIAL')
    , ('FINREP', '16.03', 'PRUDENCIAL')
    , ('FINREP', '16.04', 'PRUDENCIAL')
    , ('FINREP', '16.05', 'PRUDENCIAL')
    , ('FINREP', '16.06', 'PRUDENCIAL')
    , ('FINREP', '16.07', 'PRUDENCIAL')
    , ('FINREP', '17.01', 'CONTABILISTICO')
    , ('FINREP', '17.02', 'CONTABILISTICO')
    , ('FINREP', '17.03', 'CONTABILISTICO')
    , ('FINREP', '18.00', 'PRUDENCIAL')
    , ('FINREP', '19.00', 'PRUDENCIAL')
    , ('FINREP', '20.01', 'PRUDENCIAL')
    , ('FINREP', '20.02', 'PRUDENCIAL')
    , ('FINREP', '20.03', 'PRUDENCIAL')
    , ('FINREP', '20.04', 'PRUDENCIAL')
    , ('FINREP', '20.05', 'PRUDENCIAL')
    , ('FINREP', '20.06', 'PRUDENCIAL')
    , ('FINREP', '20.07', 'PRUDENCIAL')
    , ('FINREP', '21.00', 'PRUDENCIAL')
    , ('FINREP', '22.01', 'PRUDENCIAL')
    , ('FINREP', '22.02', 'PRUDENCIAL')
    , ('FINREP', '30.01', 'PRUDENCIAL')
    , ('FINREP', '30.02', 'PRUDENCIAL')
    , ('FINREP', '31.01', 'PRUDENCIAL')
    , ('FINREP', '31.02', 'PRUDENCIAL')
    , ('FINREP', '40.01', 'PRUDENCIAL')
    , ('FINREP', '40.02', 'PRUDENCIAL')
    , ('FINREP', '41.01', 'PRUDENCIAL')
    , ('FINREP', '41.02', 'PRUDENCIAL')
    , ('FINREP', '41.03', 'PRUDENCIAL')
    , ('FINREP', '42.00', 'PRUDENCIAL')
    , ('FINREP', '43.00', 'PRUDENCIAL')
    , ('FINREP', '44.01', 'PRUDENCIAL')
    , ('FINREP', '44.02', 'PRUDENCIAL')
    , ('FINREP', '44.03', 'PRUDENCIAL')
    , ('FINREP', '45.01', 'PRUDENCIAL')
    , ('FINREP', '45.02', 'PRUDENCIAL')
    , ('FINREP', '45.03', 'PRUDENCIAL')
    , ('FINREP', '46.00', 'PRUDENCIAL')
    AS v (framework_cd, tmplt_cd, csl_perim_typ)
) AS s
    ON t.framework_cd = s.framework_cd AND t.tmplt_cd = s.tmplt_cd
WHEN NOT MATCHED THEN INSERT (framework_cd, tmplt_cd, csl_perim_typ)
    VALUES (s.framework_cd, s.tmplt_cd, s.csl_perim_typ);

MERGE INTO dp_lgl_reg_dpm_ref_tmplt_ctry AS t
USING (
    SELECT
        framework_cd
        , tmplt_cd
        , ctry_flg
    FROM VALUES
    ('FINREP', '20.04', 1)
    , ('FINREP', '20.05.a', 1)
    , ('FINREP', '20.05.b', 1)
    , ('FINREP', '20.06', 1)
    , ('FINREP', '20.07.1', 1)
    AS v (framework_cd, tmplt_cd, ctry_flg)
) AS s
    ON t.framework_cd = s.framework_cd AND t.tmplt_cd = s.tmplt_cd
WHEN NOT MATCHED THEN INSERT (framework_cd, tmplt_cd, ctry_flg)
    VALUES (s.framework_cd, s.tmplt_cd, s.ctry_flg);

MERGE INTO dp_lgl_reg_dpm_ref_src_datapoint_typ AS t
USING (
    SELECT
        src_tbl_id
        , datapoint_typ
    FROM VALUES
    ('DP_LGL_REG_ACG_CSL_LDGR_1', 'CONTABILISTICO')
    , ('DP_LGL_REG_FACT_SCRTS_LAST', 'OPERACIONAL')
    , ('FP_M_BALANCETE_IFRS', 'CONTABILISTICO')
    , ('FP_M_FINREP', 'OPERACIONAL')
    AS v (src_tbl_id, datapoint_typ)
) AS s
    ON UPPER(TRIM(t.src_tbl_id)) = UPPER(TRIM(s.src_tbl_id))
WHEN NOT MATCHED THEN INSERT (src_tbl_id, datapoint_typ)
    VALUES (s.src_tbl_id, s.datapoint_typ);
