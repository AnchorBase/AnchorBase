
/*
&&anchor_id - uuid of anchor table
&&idmap_id - uuid of idmap table
&&anchor_rk_id - uuid of anchor rk column
&&source_system_id - uuid of anchor source_system_id column
&&etl_id - uuid of anchor etl_id column
&&idmap_rk_id - uuid of idmap rk column
&&idmap_nk_id - uuid of idmap nk column
*/


--WRITE LOG
DO $$
DECLARE  
--variables
    v_anchor_id uuid:='&&anchor_id'; 
    v_etl_id BIGINT:= (SELECT COALESCE(MAX(et.etl_id),0)+1 FROM meta.anchor_etl_log et);
    v_job_id uuid := (SELECT jb.job_id FROM meta.current_job jb);

BEGIN
--write log
INSERT INTO meta.anchor_etl_log 
    (etl_id, job_id, anchor_id, status_id, error_text, start_datetime, end_datetime) 
    VALUES 
    (v_etl_id, v_job_id, v_anchor_id, 0, NULL, CURRENT_TIMESTAMP, NULL);
END;
$$
;

--ETL
DO $$
DECLARE  
--variables
    v_etl_id BIGINT:= (SELECT MAX(et.etl_id) FROM meta.anchor_etl_log et);
    v_max_rk BIGINT:= (SELECT COALESCE(MAX("&&anchor_rk_id"),0) AS max_rk FROM am."&&anchor_id");

BEGIN

INSERT INTO am."&&anchor_id"
("&&anchor_rk_id","&&source_system_id","&&etl_id")
	SELECT 
	"&&idmap_rk_id"
	,cast(reverse(substr(reverse("&&idmap_nk_id"),1,position('@@' in reverse("&&idmap_nk_id"))-1)) as int) --source_system_id
	,v_etl_id
	FROM idmap."&&idmap_id"
	WHERE 1=1
		AND "&&idmap_rk_id">v_max_rk
;

--update log
UPDATE meta.anchor_etl_log
SET status_id=1, end_datetime=CURRENT_TIMESTAMP
WHERE etl_id=v_etl_id;

EXCEPTION WHEN OTHERS THEN 

UPDATE meta.anchor_etl_log
SET status_id=2, end_datetime=CURRENT_TIMESTAMP, error_text=SQLSTATE||': '||SQLERRM
WHERE etl_id=v_etl_id;

end; $$ 
language 'plpgsql';


