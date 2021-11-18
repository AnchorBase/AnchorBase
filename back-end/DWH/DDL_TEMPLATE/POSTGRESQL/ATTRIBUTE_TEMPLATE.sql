

CREATE TABLE am."&&attribute_id"
(
	"&&anchor_rk_id" BIGINT,
	&&attribute_name_id_and_datatype,
	"&&from_dttm_id" TIMESTAMP,
	"&&to_dttm_id" TIMESTAMP,
	"&&etl_id" BIGINT
)
PARTITION BY RANGE ("&&from_dttm_id");