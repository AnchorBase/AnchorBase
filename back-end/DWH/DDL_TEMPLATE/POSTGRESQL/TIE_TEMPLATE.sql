

CREATE TABLE am."&&tie_id"
(
	"&&anchor_rk_id" BIGINT,
	"&&link_anchor_rk_id" BIGINT,
	"&&from_dttm_id" TIMESTAMP,
	"&&to_dttm_id" TIMESTAMP,
	"&&etl_id" BIGINT
)
PARTITION BY RANGE ("&&from_dttm_id");