

/*
&&idmap_id - uuid of idmap
&&stg_table_id - uuid of source stage table
&&temp_concat_table_id - new generated uuid of first temp table
&&concat_nkey_sql - sql command which concatinates natural keys 
example: CAST("5648d121-c63e-46a8-a3fc-e44d80f0dae3" AS VARCHAR(1000))||'@@'||...
&&temp_nkey_table_id - new generated uuid of second temp table
&&idmap_nk_id - uuid of idmap natural key column
&&idmap_rk_id - uuid of idmap rk column 
&&etl_id - uuid of idmap etl_id column
&&source_id - id of source (meta.source.value ->> 'source_system_id')
*/


--WRITE LOG
DO $$
DECLARE  
--variables
    v_idmap_id uuid:='&&idmap_id'; 
    v_stg_table_id uuid:='&&stg_table_id';
    v_etl_id BIGINT:= (SELECT COALESCE(MAX(et.etl_id),0)+1 FROM meta.idmap_etl_log et);
    v_job_id uuid := (SELECT jb.job_id FROM meta.current_job jb);

BEGIN
--write log
INSERT INTO meta.idmap_etl_log 
    (etl_id, job_id, idmap_id, stg_table_id, status_id, error_text, start_datetime, end_datetime) 
    VALUES 
    (v_etl_id, v_job_id, v_idmap_id, v_stg_table_id, 0, NULL, CURRENT_TIMESTAMP, NULL);
END;
$$
;


--ETL
DO $$
DECLARE  
--variables
    v_etl_id BIGINT:= (SELECT MAX(et.etl_id) FROM meta.idmap_etl_log et);
    v_max_rk BIGINT:= (SELECT COALESCE(MAX("&&idmap_rk_id"),0) AS max_rk FROM idmap."&&idmap_id");

BEGIN

DROP TABLE IF EXISTS wrk."&&temp_concat_table_id";
--create temp table with distinct concatinated natural key
CREATE TABLE wrk."&&temp_concat_table_id" AS (
    SELECT 
    DISTINCT 
    CAST(
            &&concat_nkey_sql
            ||'@@'||
            CAST('&&source_id' AS VARCHAR(1000))
    AS VARCHAR (1000)) AS idmap_nk
    FROM stg."&&stg_table_id"
);
--drop temp table before create
DROP TABLE IF EXISTS wrk."&&temp_nkey_table_id";
--create temp table with natural keys which needs to generate rk
CREATE TABLE wrk."&&temp_nkey_table_id" AS (
    SELECT 
     nk.idmap_nk
    ,v_max_rk + ROW_NUMBER() OVER (ORDER BY 1) AS idmap_rk
    FROM wrk."&&temp_concat_table_id" as nk 
    LEFT JOIN idmap."&&idmap_id" as idmp 
        ON 1=1
        AND nk.idmap_nk=idmp."&&idmap_nk_id"
    WHERE 1=1
        AND idmp."&&idmap_rk_id" IS NULL
);

--insert data
INSERT INTO idmap."&&idmap_id"
("&&idmap_rk_id","&&idmap_nk_id","&&etl_id")
    SELECT 
    idmap_rk,
    idmap_nk,
    v_etl_id
    FROM wrk."&&temp_nkey_table_id"
;

DROP TABLE IF EXISTS wrk."&&temp_concat_table_id";
DROP TABLE IF EXISTS wrk."&&temp_nkey_table_id";

--update log
UPDATE meta.idmap_etl_log
SET status_id=1, end_datetime=CURRENT_TIMESTAMP
WHERE etl_id=v_etl_id;

EXCEPTION WHEN OTHERS THEN 

UPDATE meta.idmap_etl_log
SET status_id=2, end_datetime=CURRENT_TIMESTAMP, error_text=SQLSTATE||': '||SQLERRM
WHERE etl_id=v_etl_id;

end; $$ 
language 'plpgsql';