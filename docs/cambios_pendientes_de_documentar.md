# Cambios pendientes de documentar

Cambios del 18/11/2026. Cada uno indica qué cambia, por qué y el commit donde está.

## 1. Tablas de referencia por template: clave (framework, template)

- **Qué cambia:** `dp_lgl_reg_dpm_ref_tmplt_csl_perim` y `dp_lgl_reg_dpm_ref_tmplt_ctry` tienen la
  columna `framework_cd` y guardan el template como en la pestaña Catalog, sin la letra del
  framework (`FINREP` + `01.01`, antes `F 01.01`). El Processing cruza por las dos columnas.
- **Por qué:** antes el motor añadía la primera letra del framework al template para cruzar. FINREP y
  FCP empiezan por F, así que una plantilla del FCP con número habría cogido los valores de FINREP.
- **Regla de F 17:** pasa de `LIKE 'F 17%'` a framework FINREP + template `17.%`.
- **Valores:** los de legacy (65 perímetros de `FRP_TPMAPA_MAPEAMENTO`, 5 países de
  `FRP_ZAXIX_MAPA_COM_PAIS`), contrastados el 18/11/2026. Las subplantillas de la taxonomía nueva
  (04.02.1, 05.01, 18.00.a…) no están, igual que en legacy.
- **Commit:** `0980749`. SQL de migración, CREATE e INSERT en `sql/Motor_DPM_tablas_referencia_por_template.sql`.

## 2. Jerarquía de entidades: `dp_lgl_reg_dim_ent_hier`

- **Qué cambia:** sustituye a `D_NIV_ENTIDADE`. Columnas: `ref_dt` (antes ANOMES), `ent_id`
  (CDENTIDADE), `ent_perim_cd` (CDPERIMETRO), `ent_lvl_2_id` (CDENTNIV2), `ent_lvl_3_id` (CDENTNIV3).
- **Comprobado (PRD, 202609):** las 56 filas de `D_NIV_ENTIDADE` de Oracle están, con niveles 2 y 3
  idénticos. La tabla nueva tiene 17 entidades más (sin datos en las tablas origen de FINREP en
  202609, o con nivel 3 vacío). No se filtra por `ent_perim_rpg_cd`: legacy incluía entidades con 0.
- **A vigilar:** si alguna de las entidades nuevas empieza a tener datos, el motor la consolida en su
  nivel 3; legacy la publicaba con prefijo (`p`/`c` + código).
- **Commit:** `07d173d`.

## 3. Vista ENTIND en FINREP (diferencia intencionada con legacy)

- Legacy solo publica ENTIND (`FRP_DATAPOINTS`, VISAO = 'ENTIND') para FAE, MRL y ZRESOL1; para
  FINREP no.
- El motor la publica para todo datapoint Aggregated, también de FINREP. Decisión (Laura,
  18/11/2026): se mantiene; quien no la necesite filtra `csl_view_cd <> 'ENTIND'`.
- En ENTIND, `csl_perim_typ` sale de `dp_lgl_reg_dpm_ref_tmplt_csl_perim`; las subplantillas de la
  taxonomía nueva salen vacías.

## 4. Catálogo: columna `val_typ`

- **Qué cambia:** `dp_lgl_reg_dpm_param_ctlg` en `DE_DP_NB_LGL_REG_CREATE_TABLES` incluye `val_typ`
  (Value Type de la pestaña Catalog).
- **Por qué:** faltaba. Sin ella, Param_Load descarta el Value Type sin avisar y los porcentajes se
  publican con formato de importe.
- **Commit:** `0980749`.

## 5. Parametría: Physical Table sin lakehouse

- **Qué cambia:** en la pestaña Sources, Physical Table lleva solo el nombre de la tabla
  (`dp_lgl_reg_fact_scrts_last`, antes `DP_FET_LH_LegalAndRegulatoryReporting.dp_lgl_reg_fact_scrts_last`).
  Se actualizan el Readme del Excel y la nota de la columna.
- **Por qué:** con el prefijo, en QUA o PRD el motor buscaría el lakehouse de FET. Sin él, lee del
  lakehouse del entorno donde se ejecuta.
- **Commit:** `2da0865`.

## 6. Shortcuts del motor (`DE_DP_NB_LGL_REG_CREATE_SHORTCUTS`)

| Shortcut | Origen | Lo lee |
|---|---|---|
| `raw_dpm_param_datapoint`, `_fltr`, `_ctlg`, `_src_tbl` | `RAW {env} DATALOAD` | Param_Load, Metadata_Validation |
| `raw_dpm_exec_req` | `RAW {env} DATALOAD` | Request_Dispatcher |
| `raw_fcp_gap_datapoints` | `RAW {env} DATALOAD` | Processing (FCP) |
| `dp_fcp_bdp_global_datapoints` | `DP {env} Funding Capital Plan Processing` | Processing (FCP) |

- Los shortcuts se crean con el notebook porque la promoción entre entornos no los lleva.
- En FET no hay workspace de DataLoad: el notebook usa el de DEV.
- **Commits:** `cc49884`, `dd02bae`, `876d5b3`.

## 7. Common: celda quitada

- La celda 1 tenía un `CREATE TABLE ..._bkp_20261118`. Common se carga con `%run` desde los demás
  notebooks, así que a partir de la segunda ejecución fallaban. Se ha quitado.
- **Commit:** `e3107ad`.
