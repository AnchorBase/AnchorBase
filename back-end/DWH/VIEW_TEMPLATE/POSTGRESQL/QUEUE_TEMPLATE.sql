

CREATE OR REPLACE VIEW stg."&&queue_table_name"
AS
	SELECT 
	 @queue_attrs_aliace
	 "&&update_timestamp_id" AS update_timestamp
	,"&&etl_id" AS etl_id 
	FROM stg."&&queue_table_id"
;