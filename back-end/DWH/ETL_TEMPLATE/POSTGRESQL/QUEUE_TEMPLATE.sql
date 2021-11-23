
/*
&&stg_table_id - uuid of stg table
&&list_of_attributes_id - list of uuid of stg table attributes
EXAMPLE: "dbd3b173-ff0d-4eb9-a54c-1252bc8bf56e","cdc3b173-ff0d-4eb9-a54c-1252bc8bf56e",...
&&values_sql - sql statement of values from the source
EXAMPLE: (CAST('test_value' AS VARCHAR(100)),CAST('test_value_id' AS INT))),...
*/

--WRITE LOG
DO $$
DECLARE  
--variables
    v_stg_table_id uuid:='&&stg_table_id'; 
    v_etl_id BIGINT:= (SELECT COALESCE(MAX(et.etl_id),0)+1 FROM meta.stg_table_etl_log et);
    v_job_id uuid := (SELECT jb.job_id FROM meta.current_job jb);

BEGIN
--write log
INSERT INTO meta.stg_table_etl_log 
    (etl_id, job_id, stg_table_id, status_id, error_text, start_datetime, end_datetime) 
    VALUES 
    (v_etl_id, v_job_id, v_stg_table_id, 0, NULL, CURRENT_TIMESTAMP, NULL);
END;
$$
;

--ETL
DO $$
DECLARE  
--variables
    v_etl_id BIGINT:= (SELECT MAX(et.etl_id) FROM meta.stg_table_etl_log et);

BEGIN

INSERT INTO stg."&&stg_table_id"
(&&list_of_attributes_id)
VALUES 
&&values_sql
;

--update the last increment value
CALL update_queue_table_increment('&&stg_table_id');

--update log
UPDATE meta.stg_table_etl_log
SET status_id=1, end_datetime=CURRENT_TIMESTAMP
WHERE etl_id=v_etl_id;

EXCEPTION WHEN OTHERS THEN 

UPDATE meta.stg_table_etl_log
SET status_id=2, end_datetime=CURRENT_TIMESTAMP, error_text=SQLSTATE||': '||SQLERRM
WHERE etl_id=v_etl_id;

end; $$ 
language 'plpgsql';