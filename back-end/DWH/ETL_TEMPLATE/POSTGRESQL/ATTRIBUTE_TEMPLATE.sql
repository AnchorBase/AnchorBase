
/*
&&attribute_id - uuid of attribute table
&&idmap_id - uuid of idmap table
&&stg_table_id - uuid of source stage table
&&temp_rnum_table_id - new generated uuid of first temp table
&&concat_nkey_sql - sql command which concatinates natural keys 
example: CAST("5648d121-c63e-46a8-a3fc-e44d80f0dae3" AS VARCHAR(1000))||'@@'||...
&&idmap_nk_id - uuid of idmap natural key column
&&idmap_rk_id - uuid of idmap rk column 
&&etl_id - uuid of attribute etl_id column
&&update_timestamp_id - uuid of "update_timestamp" column in stg_table
&&source_id - uuid of source (meta.source.value ->> 'source_system_id')
&&temp_change_table_id - new generated uuid of temp table with rk and values that have changed
&&temp_insert_table_id - new generated uuid of temp table with data for insert
&&stg_attribute_column_id - uuid of stg_table column which contains attribute value
&&anchor_rk_id - uuid of attribute table rk column
&&attribute_column_id - uuid of attribute table which contains attribute value
&&from_dttm_id - uuid of attribute from_dttm column
&&to_dttm_id - uuid of attribute to_dttm column

*/

--WRITE LOG
DO $$
DECLARE  
--variables
    v_attribute_id uuid:='&&attribute_id';
    v_stg_table_id uuid:='&&stg_table_id';
    v_etl_id BIGINT:= (SELECT COALESCE(MAX(et.etl_id),0)+1 FROM meta.attribute_etl_log et);
    v_job_id uuid := (SELECT jb.job_id FROM meta.current_job jb);

BEGIN
--write log
INSERT INTO meta.attribute_etl_log 
    (etl_id, job_id, attribute_id, stg_table_id, status_id, error_text, start_datetime, end_datetime) 
    VALUES 
    (v_etl_id, v_job_id, v_attribute_id, v_stg_table_id, 0, NULL, CURRENT_TIMESTAMP, NULL);
END;
$$
;

--ETL
DO $$
DECLARE  
--variables
    v_etl_id BIGINT:= (SELECT MAX(et.etl_id) FROM meta.attribute_etl_log et);
    v_n_prt_date DATE;

BEGIN

--drop temp table before create
DROP TABLE IF EXISTS wrk."&&temp_rnum_table_id";
--temp table with concatinated natural key and row number
CREATE TABLE wrk."&&temp_rnum_table_id" AS 
(
	SELECT 
		 idmap."&&idmap_rk_id" AS idmap_rk
		,qe."&&stg_attribute_column_id" AS attribute_name
		,qe."&&update_timestamp_id" AS from_dttm
		,ROW_NUMBER() OVER (PARTITION BY idmap."&&idmap_rk_id" ORDER BY qe."&&update_timestamp_id") AS rnum 
	FROM stg."&&stg_table_id" AS qe 
	INNER JOIN idmap."&&idmap_id" AS idmap 
		ON 1=1 
		AND cast(&&concat_nkey_sql||'@@'||cast('&&source_id' as varchar(1000)) as varchar(1000))=idmap."&&idmap_nk_id"
);
--drop temp table before create
DROP TABLE IF EXISTS wrk."&&temp_change_table_id";
--create temp table with values that have changed
CREATE TABLE wrk."&&temp_change_table_id" AS 
(
	SELECT 
		 crow.idmap_rk
		,crow.attribute_name
		,crow.from_dttm
		,crow.rnum 
	FROM wrk."&&temp_rnum_table_id" AS crow 
	LEFT JOIN wrk."&&temp_rnum_table_id" AS prow 
		ON 1=1 
		AND crow.idmap_rk=prow.idmap_rk 
		AND crow.rnum=prow.rnum+1 
		AND crow.attribute_name=prow.attribute_name 
	WHERE 1=1 
		AND prow.idmap_rk IS NULL
);
--drop temp table before create
DROP TABLE IF EXISTS wrk."&&temp_insert_table_id";
--create temp table with data for insert
CREATE TABLE wrk."&&temp_insert_table_id" AS 
(
	SELECT  
	 vers.idmap_rk
	,vers.attribute_name
	,vers.from_dttm
	,COALESCE(
			 LEAD(vers.from_dttm) OVER (PARTITION BY vers.idmap_rk ORDER BY vers.RNUM ASC)-INTERVAL'1'SECOND
			,CAST('5999-12-31 00:00:00' AS TIMESTAMP)
	) AS to_dttm
	,lv."&&attribute_column_id" AS prev_attribute_name
	,lv."&&from_dttm_id" AS prev_from_dttm
	,vers.from_dttm - INTERVAL'1'SECOND AS new_to_dttm
	,lv."&&etl_id" as prev_etl_id 
	FROM wrk."&&temp_change_table_id" AS vers 
	LEFT JOIN am."&&attribute_id" AS lv 
		ON 1=1 
		AND vers.idmap_rk=lv."&&anchor_rk_id" 
		AND lv."&&to_dttm_id"=CAST('5999-12-31 00:00:00' AS TIMESTAMP)
		AND vers.rnum=1 
	WHERE 1=1 
		AND COALESCE(CAST(vers.attribute_name AS VARCHAR(1000)),CAST('###' AS CHAR(3)))<>COALESCE(CAST(lv."&&attribute_column_id" AS VARCHAR(1000)),CAST('###' AS CHAR(3)))
);

--delete old versions with 5999-12-31 to_dttm
DELETE 
	FROM am."&&attribute_id" AS attr
	USING wrk."&&temp_insert_table_id" AS tmp
WHERE 1=1
	AND attr."&&anchor_rk_id"=tmp.idmap_rk
	AND attr."&&to_dttm_id"=CAST('5999-12-31 00:00:00' AS TIMESTAMP)
;
--insert into attribute table previous values with new to_dttm
INSERT INTO am."&&attribute_id"
(
	 "&&anchor_rk_id"
	,"&&attribute_column_id"
	,"&&from_dttm_id"
	,"&&to_dttm_id"
	,"&&etl_id"
)	
SELECT 
	 idmap_rk 
	,prev_attribute_name
	,prev_from_dttm
	,new_to_dttm
	,prev_etl_id 
	FROM wrk."&&temp_insert_table_id"
	WHERE 1=1
		AND prev_from_dttm IS NOT NULL
;

--create new partitions 
FOR v_n_prt_date IN 
	SELECT 
	DISTINCT
	CAST(DATE_TRUNC('month', tmp.from_dttm)+ interval '1 month - 1 day' AS DATE) AS new_partition_date --month_end
	FROM wrk."&&temp_insert_table_id" tmp
	LEFT JOIN 
	(
		SELECT 
		TO_DATE(SUBSTR(chld.relname,POSITION('_date' IN chld.relname)+5,10),'YYYY-MM-DD') AS partition_date
		FROM pg_catalog.pg_class par
		LEFT JOIN pg_catalog.pg_inherits inh
			ON 1=1
			AND inh.inhparent=par.oid
		LEFT JOIN pg_catalog.pg_class chld
			ON 1=1
			AND inh.inhrelid=chld.oid
		WHERE 1=1
			AND par.relname='&&attribute_id'
	) AS prt 
		ON 1=1
		AND CAST(DATE_TRUNC('month', CAST(tmp.from_dttm AS DATE)) + INTERVAL '1 month - 1 day' AS DATE)=prt.partition_date
	WHERE 1=1
		AND prt.partition_date IS NULL
	LOOP 
		EXECUTE 'CREATE TABLE "&&attribute_id_date'||v_n_prt_date||'"'||
				'PARTITION OF am."&&attribute_id" FOR VALUES '||
				'FROM ('''||CAST(DATE_TRUNC('month',CAST(v_n_prt_date AS DATE)) AS DATE)||' 00:00:00'') TO ('''||v_n_prt_date||' 23:59:59'');';
	END LOOP;

--insert into attribute table new values
INSERT INTO am."&&attribute_id"
(
	 "&&anchor_rk_id"
	,"&&attribute_column_id"
	,"&&from_dttm_id"
	,"&&to_dttm_id"
	,"&&etl_id"
)	
SELECT 
	 idmap_rk 
	,attribute_name
	,from_dttm
	,to_dttm
	,v_etl_id 
	FROM wrk."&&temp_insert_table_id"
;

DROP TABLE IF EXISTS wrk."&&temp_rnum_table_id";
DROP TABLE IF EXISTS wrk."&&temp_change_table_id";
DROP TABLE IF EXISTS wrk."&&temp_insert_table_id";

--update log
UPDATE meta.attribute_etl_log
SET status_id=1, end_datetime=CURRENT_TIMESTAMP
WHERE etl_id=v_etl_id;

EXCEPTION WHEN OTHERS THEN 

UPDATE meta.attribute_etl_log
SET status_id=2, end_datetime=CURRENT_TIMESTAMP, error_text=SQLSTATE||': '||SQLERRM
WHERE etl_id=v_etl_id;

end; $$ 
language 'plpgsql';

