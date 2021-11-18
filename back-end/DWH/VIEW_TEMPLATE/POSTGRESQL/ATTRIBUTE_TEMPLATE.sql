

CREATE OR REPLACE VIEW am."&&attribute_name"
AS
	SELECT 
	 "&&anchor_rk_id" AS "&&anchor_rk_name"
	,&&atribute_column
	,"&&from_dttm_id" AS from_dttm
	,"&&to_dttm_id" AS to_dttm
	,"&&etl_id" AS etl_id 
	FROM am."&&attribute_id"
;