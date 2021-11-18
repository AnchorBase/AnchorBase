

CREATE OR REPLACE VIEW am."&&tie_name"
AS
	SELECT 
	 "&&anchor_rk_id" AS "&&anchor_rk_name"
	,"&&link_anchor_rk_id" AS "&&link_anchor_column_name"
	,"&&from_dttm_id" AS from_dttm
	,"&&to_dttm_id" AS to_dttm
	,"&&etl_id" AS etl_id 
	FROM am."&&tie_id"
;