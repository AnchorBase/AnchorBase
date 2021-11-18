
CREATE OR REPLACE VIEW idmap."&&idmap_name"
AS
	SELECT 
	 "&&idmap_rk_id" AS "&&idmap_rk_name"
	,"&&idmap_nk_id" AS "&&idmap_nk_name"
	,"&&etl_id" AS etl_id 
	FROM idmap."&&idmap_id"
;