
--insert the row of starting the job
CREATE OR REPLACE PROCEDURE start_job()
LANGUAGE PLPGSQL
AS 
$$
DECLARE 
	v_cur_job_cnt INT;
BEGIN 
	--check the absence of the job which is in progress
	SELECT COUNT(1) INTO v_cur_job_cnt FROM meta.current_job WHERE status_id=0;
	IF v_cur_job_cnt>0 THEN 
		RAISE EXCEPTION 'There is already the job which is in progress!';
	ELSE
		INSERT INTO meta.current_job
		(job_id, status_id, start_datetime)
		VALUES 
		(uuid_generate_v4(), 0, CURRENT_TIMESTAMP);
	END IF;
END;
$$
