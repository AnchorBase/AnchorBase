--find the increment value of the last load
CREATE OR REPLACE FUNCTION stg_table_increment(v_stg_table_id uuid)
RETURNS TIMESTAMP
LANGUAGE PLPGSQL
AS 
$$
DECLARE 
	v_increment_datetime TIMESTAMP;
BEGIN
	SELECT COALESCE(MAX(qti.increment_datetime),CAST('1900-01-01 00:00:00' AS TIMESTAMP))
	INTO v_increment_datetime
 	FROM meta.stg_table_increment qti
	WHERE qti.stg_table_id=v_stg_table_id;
	
	RETURN v_increment_datetime;
END;
$$;

--find the max increment value in stg table
CREATE OR REPLACE FUNCTION max_stg_table_increment(v_stg_table_id uuid)
RETURNS TIMESTAMP
LANGUAGE PLPGSQL
AS 
$$
DECLARE 
	v_max_increment_datetime TIMESTAMP;
BEGIN 
	EXECUTE 'SELECT MAX(src.update_timestamp) FROM stg."'||v_stg_table_id||'" AS src'
	INTO v_max_increment_datetime;
	
	RETURN v_max_increment_datetime;
END;
$$;


--update the increment value of the last load
CREATE OR REPLACE PROCEDURE update_queue_table_increment(v_table_id uuid)
LANGUAGE PLPGSQL
AS 
$$
DECLARE
	v_incr_dttm TIMESTAMP; 
	v_prev_incr_dttm TIMESTAMP; 
BEGIN
	SELECT *
	INTO v_incr_dttm 
	FROM max_queue_table_increment(v_table_id);

	SELECT *
	INTO v_prev_incr_dttm 
	FROM queue_table_increment(v_table_id);

	DELETE FROM meta.queue_table_increment WHERE queue_table_id=v_table_id;

	INSERT INTO meta.queue_table_increment (queue_table_id,increment_datetime)
	VALUES 
	(v_table_id, COALESCE(v_incr_dttm,v_prev_incr_dttm));
END;
$$;