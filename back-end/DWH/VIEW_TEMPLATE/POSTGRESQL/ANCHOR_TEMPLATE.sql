

CREATE OR REPLACE VIEW am."&&anchor_name"
AS
	SELECT 
	 "&&anchor_rk_id" AS "&&anchor_rk_name"
	,"&&source_id" AS source_system_id
	,"&&etl_id" AS etl_id 
	FROM am."&&anchor_id"
;