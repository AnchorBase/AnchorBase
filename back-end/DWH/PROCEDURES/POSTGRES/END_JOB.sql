
--end the job - update the end_datetime and put the row into meta.job_log
CREATE OR REPLACE PROCEDURE end_job(v_status_id INT)
--v_status_id: 1 - Success, 2 - Error
LANGUAGE PLPGSQL
AS 
$$
BEGIN 
	INSERT INTO meta.job_log 
	(job_id, status_id, start_datetime, end_datetime)
	SELECT job_id, v_status_id, start_datetime, CURRENT_TIMESTAMP
	FROM meta.CURRENT_JOB;

	DELETE FROM meta.CURRENT_JOB;
END;
$$
