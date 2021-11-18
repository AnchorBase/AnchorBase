
CREATE EXTENSION IF NOT EXISTS uuid-ossp;

-----------
--TEST 1.а
-----------

--DDL
create table anchor1 
(
	acnhor1_rk uuid,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr1 
(
	acnhor1_rk uuid,
	attr1_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr2
(
	acnhor1_rk uuid,
	anchor1_attr2 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr3
(
	acnhor1_rk uuid,
	anchor1_attr3 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr4
(
	acnhor1_rk uuid,
	anchor1_attr4 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr5
(
	acnhor1_rk uuid,
	anchor1_attr5 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr6
(
	acnhor1_rk uuid,
	anchor1_attr6 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr7
(
	acnhor1_rk uuid,
	anchor1_attr7 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr8
(
	acnhor1_rk uuid,
	anchor1_attr8 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr9
(
	acnhor1_rk uuid,
	anchor1_attr9 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr10
(
	acnhor1_rk uuid,
	anchor1_attr10 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr11
(
	acnhor1_rk uuid,
	anchor1_attr11 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr12
(
	acnhor1_rk uuid,
	anchor1_attr12 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr13
(
	acnhor1_rk uuid,
	anchor1_attr13 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr14
(
	acnhor1_rk uuid,
	anchor1_attr14 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr15
(
	acnhor1_rk uuid,
	anchor1_attr15 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr16
(
	acnhor1_rk uuid,
	anchor1_attr16 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr17
(
	acnhor1_rk uuid,
	anchor1_attr17 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr18
(
	acnhor1_rk uuid,
	anchor1_attr18 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr19
(
	acnhor1_rk uuid,
	anchor1_attr19 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr20
(
	acnhor1_rk uuid,
	anchor1_attr20 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr21
(
	acnhor1_rk uuid,
	anchor1_attr21 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr22
(
	acnhor1_rk uuid,
	anchor1_attr22 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr23
(
	acnhor1_rk uuid,
	anchor1_attr23 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

create table anchor1_attr24
(
	acnhor1_rk uuid,
	anchor1_attr24 integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
);

--100 000 row
--5 power

WITH recursive r AS (
SELECT
	uuid_generate_v4()  as acnhor1_rk,
	current_timestamp as processed_dtmm,
	uuid_generate_v4() as etl_id,
	1 as num
UNION ALL
SELECT
	uuid_generate_v4()  as acnhor1_rk,
	current_timestamp as processed_dtmm,
	uuid_generate_v4() as etl_id,
	num+1 as num
FROM
	r
WHERE
	num < 100000
)

insert into anchor1
select acnhor1_rk, processed_dtmm, etl_id
from r 


insert into anchor1_attr1
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr2
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr3
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr4
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr5
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr6
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr7
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr8
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr9
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr10
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr11
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr12
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr13
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr14
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr15
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr16
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr17
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr18
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr19
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr20
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr21
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr22
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr23
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr24
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), current_timestamp, etl_id 
	from anchor1;

------------
--view
------------
create view anchor1_attr1_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr1;
create view anchor1_attr2_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr2;
create view anchor1_attr3_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr3;
create view anchor1_attr4_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr4;
create view anchor1_attr5_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr5;
create view anchor1_attr6_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr6;
create view anchor1_attr7_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr7;
create view anchor1_attr8_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr8;
create view anchor1_attr9_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr9;
create view anchor1_attr10_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr10;
create view anchor1_attr11_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr11;
create view anchor1_attr12_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr12;
create view anchor1_attr13_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr13;
create view anchor1_attr14_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr14;
create view anchor1_attr15_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr15;
create view anchor1_attr16_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr16;
create view anchor1_attr17_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr17;
create view anchor1_attr18_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr18;
create view anchor1_attr19_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr19;
create view anchor1_attr20_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr20;
create view anchor1_attr21_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr21;
create view anchor1_attr22_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr22;
create view anchor1_attr23_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr23;
create view anchor1_attr24_v as 
	select *, update_dttm as from_dttm, coalesce(
		lead(update_dttm) over (partition by acnhor1_rk order by update_dttm asc), cast('5999-12-31 00:00:00' as timestamp)
		) as to_dttm
	from anchor1_attr24;

--test 1.b
-----------------
--last vers table
-----------------
create table anchor1_attr1_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr1
		group by acnhor1_rk
		);
create table anchor1_attr2_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr2
		group by acnhor1_rk
		);
create table anchor1_attr3_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr3
		group by acnhor1_rk
		);
create table anchor1_attr4_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr4
		group by acnhor1_rk
		);
create table anchor1_attr5_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr5
		group by acnhor1_rk
		);
create table anchor1_attr6_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr6
		group by acnhor1_rk
		);
create table anchor1_attr7_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr7
		group by acnhor1_rk
		);
create table anchor1_attr8_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr8
		group by acnhor1_rk
		);
create table anchor1_attr9_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr9
		group by acnhor1_rk
		);
create table anchor1_attr10_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr10
		group by acnhor1_rk
		);
create table anchor1_attr11_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr11
		group by acnhor1_rk
		);
create table anchor1_attr12_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr12
		group by acnhor1_rk
		);
create table anchor1_attr13_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr13
		group by acnhor1_rk
		);
create table anchor1_attr14_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr14
		group by acnhor1_rk
		);
create table anchor1_attr15_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr15
		group by acnhor1_rk
		);
create table anchor1_attr16_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr16
		group by acnhor1_rk
		);
create table anchor1_attr17_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr17
		group by acnhor1_rk
		);
create table anchor1_attr18_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr18
		group by acnhor1_rk
		);
create table anchor1_attr19_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr19
		group by acnhor1_rk
		);
create table anchor1_attr20_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr20
		group by acnhor1_rk
		);
create table anchor1_attr21_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr21
		group by acnhor1_rk
		);
create table anchor1_attr22_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr22
		group by acnhor1_rk
		);
create table anchor1_attr23_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr23
		group by acnhor1_rk
		);
create table anchor1_attr24_mx as 
	(
		select acnhor1_rk, max(update_dttm) as last_update_dttm 
		from anchor1_attr24
		group by acnhor1_rk
		);

-----------------
--last vers view
-----------------
create view anchor1_attr1_lv 
	as 
	select a.*
	from anchor1_attr1 a 
	inner join anchor1_attr1_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr2_lv 
	as 
	select a.*
	from anchor1_attr2 a 
	inner join anchor1_attr2_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr3_lv 
	as 
	select a.*
	from anchor1_attr3 a 
	inner join anchor1_attr3_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr4_lv 
	as 
	select a.*
	from anchor1_attr4 a 
	inner join anchor1_attr4_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr5_lv 
	as 
	select a.*
	from anchor1_attr5 a 
	inner join anchor1_attr5_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr6_lv 
	as 
	select a.*
	from anchor1_attr6 a 
	inner join anchor1_attr6_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr7_lv 
	as 
	select a.*
	from anchor1_attr7 a 
	inner join anchor1_attr7_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr8_lv 
	as 
	select a.*
	from anchor1_attr8 a 
	inner join anchor1_attr8_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr9_lv 
	as 
	select a.*
	from anchor1_attr9 a 
	inner join anchor1_attr9_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr10_lv 
	as 
	select a.*
	from anchor1_attr10 a 
	inner join anchor1_attr10_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr11_lv 
	as 
	select a.*
	from anchor1_attr11 a 
	inner join anchor1_attr11_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr12_lv 
	as 
	select a.*
	from anchor1_attr12 a 
	inner join anchor1_attr12_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr13_lv 
	as 
	select a.*
	from anchor1_attr13 a 
	inner join anchor1_attr13_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr14_lv 
	as 
	select a.*
	from anchor1_attr14 a 
	inner join anchor1_attr14_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr15_lv 
	as 
	select a.*
	from anchor1_attr15 a 
	inner join anchor1_attr15_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr16_lv 
	as 
	select a.*
	from anchor1_attr16 a 
	inner join anchor1_attr16_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr17_lv 
	as 
	select a.*
	from anchor1_attr17 a 
	inner join anchor1_attr17_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr18_lv 
	as 
	select a.*
	from anchor1_attr18 a 
	inner join anchor1_attr18_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr19_lv 
	as 
	select a.*
	from anchor1_attr19 a 
	inner join anchor1_attr19_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr20_lv 
	as 
	select a.*
	from anchor1_attr20 a 
	inner join anchor1_attr20_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr21_lv 
	as 
	select a.*
	from anchor1_attr21 a 
	inner join anchor1_attr21_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr22_lv 
	as 
	select a.*
	from anchor1_attr22 a 
	inner join anchor1_attr22_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr23_lv 
	as 
	select a.*
	from anchor1_attr23 a 
	inner join anchor1_attr23_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;
create view anchor1_attr24_lv 
	as 
	select a.*
	from anchor1_attr24 a 
	inner join anchor1_attr24_mx b 
	on a.acnhor1_rk=b.acnhor1_rk
           and a.update_dttm=b.last_update_dttm
;

--test 1.c
create table anchor1 
(
	acnhor1_rk uuid,
	processed_dtmm timestamp,
	etl_id uuid
);
create index on anchor1 (acnhor1_rk);


create table anchor1_attr1 
(
	acnhor1_rk uuid,
	attr1_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr1_07102021 PARTITION OF anchor1_attr1 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr1_06102021 PARTITION OF anchor1_attr1 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr1_05102021 PARTITION OF anchor1_attr1 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr1_09102021 PARTITION OF anchor1_attr1 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr1_08102021 PARTITION OF anchor1_attr1 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr1_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr1_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr1_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr1_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr1_08102021 (acnhor1_rk);
create table anchor1_attr2 
(
	acnhor1_rk uuid,
	attr2_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr2_07102021 PARTITION OF anchor1_attr2 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr2_06102021 PARTITION OF anchor1_attr2 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr2_05102021 PARTITION OF anchor1_attr2 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr2_09102021 PARTITION OF anchor1_attr2 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr2_08102021 PARTITION OF anchor1_attr2 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr2_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr2_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr2_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr2_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr2_08102021 (acnhor1_rk);
create table anchor1_attr3 
(
	acnhor1_rk uuid,
	attr3_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr3_07102021 PARTITION OF anchor1_attr3 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr3_06102021 PARTITION OF anchor1_attr3 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr3_05102021 PARTITION OF anchor1_attr3 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr3_09102021 PARTITION OF anchor1_attr3 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr3_08102021 PARTITION OF anchor1_attr3 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr3_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr3_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr3_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr3_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr3_08102021 (acnhor1_rk);
create table anchor1_attr4 
(
	acnhor1_rk uuid,
	attr4_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr4_07102021 PARTITION OF anchor1_attr4 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr4_06102021 PARTITION OF anchor1_attr4 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr4_05102021 PARTITION OF anchor1_attr4 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr4_09102021 PARTITION OF anchor1_attr4 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr4_08102021 PARTITION OF anchor1_attr4 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr4_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr4_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr4_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr4_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr4_08102021 (acnhor1_rk);
create table anchor1_attr5 
(
	acnhor1_rk uuid,
	attr5_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr5_07102021 PARTITION OF anchor1_attr5 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr5_06102021 PARTITION OF anchor1_attr5 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr5_05102021 PARTITION OF anchor1_attr5 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr5_09102021 PARTITION OF anchor1_attr5 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr5_08102021 PARTITION OF anchor1_attr5 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr5_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr5_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr5_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr5_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr5_08102021 (acnhor1_rk);
create table anchor1_attr6 
(
	acnhor1_rk uuid,
	attr6_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr6_07102021 PARTITION OF anchor1_attr6 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr6_06102021 PARTITION OF anchor1_attr6 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr6_05102021 PARTITION OF anchor1_attr6 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr6_09102021 PARTITION OF anchor1_attr6 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr6_08102021 PARTITION OF anchor1_attr6 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr6_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr6_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr6_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr6_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr6_08102021 (acnhor1_rk);
create table anchor1_attr7 
(
	acnhor1_rk uuid,
	attr7_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr7_07102021 PARTITION OF anchor1_attr7 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr7_06102021 PARTITION OF anchor1_attr7 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr7_05102021 PARTITION OF anchor1_attr7 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr7_09102021 PARTITION OF anchor1_attr7 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr7_08102021 PARTITION OF anchor1_attr7 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr7_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr7_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr7_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr7_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr7_08102021 (acnhor1_rk);
create table anchor1_attr8 
(
	acnhor1_rk uuid,
	attr8_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr8_07102021 PARTITION OF anchor1_attr8 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr8_06102021 PARTITION OF anchor1_attr8 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr8_05102021 PARTITION OF anchor1_attr8 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr8_09102021 PARTITION OF anchor1_attr8 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr8_08102021 PARTITION OF anchor1_attr8 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr8_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr8_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr8_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr8_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr8_08102021 (acnhor1_rk);
create table anchor1_attr9 
(
	acnhor1_rk uuid,
	attr9_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr9_07102021 PARTITION OF anchor1_attr9 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr9_06102021 PARTITION OF anchor1_attr9 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr9_05102021 PARTITION OF anchor1_attr9 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr9_09102021 PARTITION OF anchor1_attr9 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr9_08102021 PARTITION OF anchor1_attr9 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr9_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr9_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr9_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr9_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr9_08102021 (acnhor1_rk);
create table anchor1_attr10 
(
	acnhor1_rk uuid,
	attr10_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr10_07102021 PARTITION OF anchor1_attr10 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr10_06102021 PARTITION OF anchor1_attr10 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr10_05102021 PARTITION OF anchor1_attr10 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr10_09102021 PARTITION OF anchor1_attr10 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr10_08102021 PARTITION OF anchor1_attr10 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr10_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr10_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr10_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr10_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr10_08102021 (acnhor1_rk);
create table anchor1_attr11 
(
	acnhor1_rk uuid,
	attr11_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr11_07102021 PARTITION OF anchor1_attr11 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr11_06102021 PARTITION OF anchor1_attr11 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr11_05102021 PARTITION OF anchor1_attr11 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr11_09102021 PARTITION OF anchor1_attr11 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr11_08102021 PARTITION OF anchor1_attr11 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr11_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr11_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr11_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr11_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr11_08102021 (acnhor1_rk);
create table anchor1_attr12 
(
	acnhor1_rk uuid,
	attr12_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr12_07102021 PARTITION OF anchor1_attr12 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr12_06102021 PARTITION OF anchor1_attr12 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr12_05102021 PARTITION OF anchor1_attr12 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr12_09102021 PARTITION OF anchor1_attr12 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr12_08102021 PARTITION OF anchor1_attr12 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr12_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr12_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr12_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr12_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr12_08102021 (acnhor1_rk);
create table anchor1_attr13 
(
	acnhor1_rk uuid,
	attr13_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr13_07102021 PARTITION OF anchor1_attr13 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr13_06102021 PARTITION OF anchor1_attr13 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr13_05102021 PARTITION OF anchor1_attr13 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr13_09102021 PARTITION OF anchor1_attr13 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr13_08102021 PARTITION OF anchor1_attr13 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr13_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr13_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr13_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr13_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr13_08102021 (acnhor1_rk);
create table anchor1_attr14 
(
	acnhor1_rk uuid,
	attr14_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr14_07102021 PARTITION OF anchor1_attr14 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr14_06102021 PARTITION OF anchor1_attr14 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr14_05102021 PARTITION OF anchor1_attr14 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr14_09102021 PARTITION OF anchor1_attr14 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr14_08102021 PARTITION OF anchor1_attr14 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr14_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr14_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr14_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr14_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr14_08102021 (acnhor1_rk);
create table anchor1_attr15 
(
	acnhor1_rk uuid,
	attr15_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr15_07102021 PARTITION OF anchor1_attr15 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr15_06102021 PARTITION OF anchor1_attr15 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr15_05102021 PARTITION OF anchor1_attr15 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr15_09102021 PARTITION OF anchor1_attr15 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr15_08102021 PARTITION OF anchor1_attr15 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr15_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr15_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr15_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr15_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr15_08102021 (acnhor1_rk);
create table anchor1_attr16 
(
	acnhor1_rk uuid,
	attr16_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr16_07102021 PARTITION OF anchor1_attr16 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr16_06102021 PARTITION OF anchor1_attr16 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr16_05102021 PARTITION OF anchor1_attr16 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr16_09102021 PARTITION OF anchor1_attr16 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr16_08102021 PARTITION OF anchor1_attr16 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr16_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr16_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr16_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr16_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr16_08102021 (acnhor1_rk);
create table anchor1_attr17 
(
	acnhor1_rk uuid,
	attr17_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr17_07102021 PARTITION OF anchor1_attr17 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr17_06102021 PARTITION OF anchor1_attr17 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr17_05102021 PARTITION OF anchor1_attr17 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr17_09102021 PARTITION OF anchor1_attr17 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr17_08102021 PARTITION OF anchor1_attr17 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr17_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr17_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr17_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr17_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr17_08102021 (acnhor1_rk);
create table anchor1_attr18 
(
	acnhor1_rk uuid,
	attr18_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr18_07102021 PARTITION OF anchor1_attr18 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr18_06102021 PARTITION OF anchor1_attr18 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr18_05102021 PARTITION OF anchor1_attr18 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr18_09102021 PARTITION OF anchor1_attr18 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr18_08102021 PARTITION OF anchor1_attr18 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr18_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr18_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr18_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr18_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr18_08102021 (acnhor1_rk);
create table anchor1_attr19 
(
	acnhor1_rk uuid,
	attr19_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr19_07102021 PARTITION OF anchor1_attr19 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr19_06102021 PARTITION OF anchor1_attr19 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr19_05102021 PARTITION OF anchor1_attr19 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr19_09102021 PARTITION OF anchor1_attr19 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr19_08102021 PARTITION OF anchor1_attr19 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr19_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr19_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr19_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr19_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr19_08102021 (acnhor1_rk);
create table anchor1_attr20 
(
	acnhor1_rk uuid,
	attr20_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr20_07102021 PARTITION OF anchor1_attr20 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr20_06102021 PARTITION OF anchor1_attr20 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr20_05102021 PARTITION OF anchor1_attr20 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr20_09102021 PARTITION OF anchor1_attr20 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr20_08102021 PARTITION OF anchor1_attr20 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr20_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr20_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr20_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr20_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr20_08102021 (acnhor1_rk);
create table anchor1_attr21 
(
	acnhor1_rk uuid,
	attr21_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr21_07102021 PARTITION OF anchor1_attr21 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr21_06102021 PARTITION OF anchor1_attr21 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr21_05102021 PARTITION OF anchor1_attr21 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr21_09102021 PARTITION OF anchor1_attr21 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr21_08102021 PARTITION OF anchor1_attr21 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr21_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr21_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr21_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr21_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr21_08102021 (acnhor1_rk);
create table anchor1_attr22 
(
	acnhor1_rk uuid,
	attr22_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr22_07102021 PARTITION OF anchor1_attr22 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr22_06102021 PARTITION OF anchor1_attr22 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr22_05102021 PARTITION OF anchor1_attr22 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr22_09102021 PARTITION OF anchor1_attr22 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr22_08102021 PARTITION OF anchor1_attr22 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr22_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr22_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr22_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr22_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr22_08102021 (acnhor1_rk);
create table anchor1_attr23 
(
	acnhor1_rk uuid,
	attr23_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr23_07102021 PARTITION OF anchor1_attr23 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr23_06102021 PARTITION OF anchor1_attr23 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr23_05102021 PARTITION OF anchor1_attr23 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr23_09102021 PARTITION OF anchor1_attr23 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr23_08102021 PARTITION OF anchor1_attr23 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr23_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr23_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr23_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr23_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr23_08102021 (acnhor1_rk);
create table anchor1_attr24 
(
	acnhor1_rk uuid,
	attr24_value integer,
	update_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (update_dttm);
CREATE TABLE anchor1_attr24_07102021 PARTITION OF anchor1_attr24 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr24_06102021 PARTITION OF anchor1_attr24 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr24_05102021 PARTITION OF anchor1_attr24 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr24_09102021 PARTITION OF anchor1_attr24 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr24_08102021 PARTITION OF anchor1_attr24 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr24_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr24_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr24_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr24_09102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr24_08102021 (acnhor1_rk);




-----------------------
--1e
create table anchor1_attr1
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr1_07102021 PARTITION OF anchor1_attr1 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr1_06102021 PARTITION OF anchor1_attr1 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr1_05102021 PARTITION OF anchor1_attr1 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr1_04102021 PARTITION OF anchor1_attr1 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr1_08102021 PARTITION OF anchor1_attr1 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr1_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr1_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr1_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr1_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr1_08102021 (acnhor1_rk);
create table anchor1_attr2
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr2_07102021 PARTITION OF anchor1_attr2 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr2_06102021 PARTITION OF anchor1_attr2 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr2_05102021 PARTITION OF anchor1_attr2 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr2_04102021 PARTITION OF anchor1_attr2 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr2_08102021 PARTITION OF anchor1_attr2 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr2_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr2_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr2_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr2_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr2_08102021 (acnhor1_rk);
create table anchor1_attr3
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr3_07102021 PARTITION OF anchor1_attr3 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr3_06102021 PARTITION OF anchor1_attr3 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr3_05102021 PARTITION OF anchor1_attr3 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr3_04102021 PARTITION OF anchor1_attr3 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr3_08102021 PARTITION OF anchor1_attr3 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr3_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr3_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr3_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr3_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr3_08102021 (acnhor1_rk);
create table anchor1_attr4
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr4_07102021 PARTITION OF anchor1_attr4 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr4_06102021 PARTITION OF anchor1_attr4 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr4_05102021 PARTITION OF anchor1_attr4 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr4_04102021 PARTITION OF anchor1_attr4 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr4_08102021 PARTITION OF anchor1_attr4 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr4_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr4_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr4_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr4_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr4_08102021 (acnhor1_rk);
create table anchor1_attr5
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr5_07102021 PARTITION OF anchor1_attr5 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr5_06102021 PARTITION OF anchor1_attr5 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr5_05102021 PARTITION OF anchor1_attr5 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr5_04102021 PARTITION OF anchor1_attr5 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr5_08102021 PARTITION OF anchor1_attr5 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr5_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr5_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr5_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr5_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr5_08102021 (acnhor1_rk);
create table anchor1_attr6
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr6_07102021 PARTITION OF anchor1_attr6 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr6_06102021 PARTITION OF anchor1_attr6 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr6_05102021 PARTITION OF anchor1_attr6 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr6_04102021 PARTITION OF anchor1_attr6 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr6_08102021 PARTITION OF anchor1_attr6 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr6_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr6_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr6_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr6_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr6_08102021 (acnhor1_rk);
create table anchor1_attr7
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr7_07102021 PARTITION OF anchor1_attr7 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr7_06102021 PARTITION OF anchor1_attr7 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr7_05102021 PARTITION OF anchor1_attr7 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr7_04102021 PARTITION OF anchor1_attr7 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr7_08102021 PARTITION OF anchor1_attr7 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr7_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr7_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr7_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr7_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr7_08102021 (acnhor1_rk);
create table anchor1_attr8
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr8_07102021 PARTITION OF anchor1_attr8 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr8_06102021 PARTITION OF anchor1_attr8 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr8_05102021 PARTITION OF anchor1_attr8 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr8_04102021 PARTITION OF anchor1_attr8 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr8_08102021 PARTITION OF anchor1_attr8 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr8_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr8_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr8_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr8_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr8_08102021 (acnhor1_rk);
create table anchor1_attr9
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr9_07102021 PARTITION OF anchor1_attr9 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr9_06102021 PARTITION OF anchor1_attr9 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr9_05102021 PARTITION OF anchor1_attr9 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr9_04102021 PARTITION OF anchor1_attr9 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr9_08102021 PARTITION OF anchor1_attr9 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr9_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr9_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr9_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr9_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr9_08102021 (acnhor1_rk);
create table anchor1_attr10
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr10_07102021 PARTITION OF anchor1_attr10 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr10_06102021 PARTITION OF anchor1_attr10 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr10_05102021 PARTITION OF anchor1_attr10 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr10_04102021 PARTITION OF anchor1_attr10 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr10_08102021 PARTITION OF anchor1_attr10 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr10_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr10_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr10_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr10_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr10_08102021 (acnhor1_rk);
create table anchor1_attr11
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr11_07102021 PARTITION OF anchor1_attr11 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr11_06102021 PARTITION OF anchor1_attr11 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr11_05102021 PARTITION OF anchor1_attr11 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr11_04102021 PARTITION OF anchor1_attr11 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr11_08102021 PARTITION OF anchor1_attr11 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr11_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr11_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr11_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr11_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr11_08102021 (acnhor1_rk);
create table anchor1_attr12
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr12_07102021 PARTITION OF anchor1_attr12 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr12_06102021 PARTITION OF anchor1_attr12 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr12_05102021 PARTITION OF anchor1_attr12 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr12_04102021 PARTITION OF anchor1_attr12 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr12_08102021 PARTITION OF anchor1_attr12 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr12_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr12_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr12_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr12_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr12_08102021 (acnhor1_rk);
create table anchor1_attr13
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr13_07102021 PARTITION OF anchor1_attr13 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr13_06102021 PARTITION OF anchor1_attr13 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr13_05102021 PARTITION OF anchor1_attr13 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr13_04102021 PARTITION OF anchor1_attr13 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr13_08102021 PARTITION OF anchor1_attr13 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr13_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr13_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr13_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr13_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr13_08102021 (acnhor1_rk);
create table anchor1_attr14
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr14_07102021 PARTITION OF anchor1_attr14 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr14_06102021 PARTITION OF anchor1_attr14 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr14_05102021 PARTITION OF anchor1_attr14 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr14_04102021 PARTITION OF anchor1_attr14 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr14_08102021 PARTITION OF anchor1_attr14 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr14_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr14_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr14_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr14_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr14_08102021 (acnhor1_rk);
create table anchor1_attr15
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr15_07102021 PARTITION OF anchor1_attr15 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr15_06102021 PARTITION OF anchor1_attr15 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr15_05102021 PARTITION OF anchor1_attr15 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr15_04102021 PARTITION OF anchor1_attr15 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr15_08102021 PARTITION OF anchor1_attr15 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr15_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr15_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr15_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr15_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr15_08102021 (acnhor1_rk);
create table anchor1_attr16
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr16_07102021 PARTITION OF anchor1_attr16 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr16_06102021 PARTITION OF anchor1_attr16 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr16_05102021 PARTITION OF anchor1_attr16 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr16_04102021 PARTITION OF anchor1_attr16 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr16_08102021 PARTITION OF anchor1_attr16 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr16_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr16_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr16_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr16_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr16_08102021 (acnhor1_rk);
create table anchor1_attr17
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr17_07102021 PARTITION OF anchor1_attr17 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr17_06102021 PARTITION OF anchor1_attr17 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr17_05102021 PARTITION OF anchor1_attr17 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr17_04102021 PARTITION OF anchor1_attr17 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr17_08102021 PARTITION OF anchor1_attr17 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr17_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr17_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr17_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr17_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr17_08102021 (acnhor1_rk);
create table anchor1_attr18
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr18_07102021 PARTITION OF anchor1_attr18 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr18_06102021 PARTITION OF anchor1_attr18 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr18_05102021 PARTITION OF anchor1_attr18 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr18_04102021 PARTITION OF anchor1_attr18 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr18_08102021 PARTITION OF anchor1_attr18 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr18_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr18_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr18_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr18_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr18_08102021 (acnhor1_rk);
create table anchor1_attr19
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr19_07102021 PARTITION OF anchor1_attr19 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr19_06102021 PARTITION OF anchor1_attr19 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr19_05102021 PARTITION OF anchor1_attr19 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr19_04102021 PARTITION OF anchor1_attr19 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr19_08102021 PARTITION OF anchor1_attr19 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr19_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr19_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr19_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr19_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr19_08102021 (acnhor1_rk);
create table anchor1_attr20
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr20_07102021 PARTITION OF anchor1_attr20 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr20_06102021 PARTITION OF anchor1_attr20 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr20_05102021 PARTITION OF anchor1_attr20 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr20_04102021 PARTITION OF anchor1_attr20 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr20_08102021 PARTITION OF anchor1_attr20 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr20_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr20_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr20_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr20_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr20_08102021 (acnhor1_rk);
create table anchor1_attr21
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr21_07102021 PARTITION OF anchor1_attr21 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr21_06102021 PARTITION OF anchor1_attr21 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr21_05102021 PARTITION OF anchor1_attr21 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr21_04102021 PARTITION OF anchor1_attr21 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr21_08102021 PARTITION OF anchor1_attr21 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr21_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr21_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr21_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr21_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr21_08102021 (acnhor1_rk);
create table anchor1_attr22
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr22_07102021 PARTITION OF anchor1_attr22 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr22_06102021 PARTITION OF anchor1_attr22 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr22_05102021 PARTITION OF anchor1_attr22 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr22_04102021 PARTITION OF anchor1_attr22 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr22_08102021 PARTITION OF anchor1_attr22 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr22_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr22_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr22_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr22_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr22_08102021 (acnhor1_rk);
create table anchor1_attr23
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr23_07102021 PARTITION OF anchor1_attr23 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr23_06102021 PARTITION OF anchor1_attr23 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr23_05102021 PARTITION OF anchor1_attr23 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr23_04102021 PARTITION OF anchor1_attr23 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr23_08102021 PARTITION OF anchor1_attr23 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr23_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr23_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr23_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr23_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr23_08102021 (acnhor1_rk);
create table anchor1_attr24
(
	acnhor1_rk uuid,
	attr3_value integer,
            from_dttm timestamp,
            to_dttm timestamp,
	processed_dtmm timestamp,
	etl_id uuid
) partition by range (from_dttm);
CREATE TABLE anchor1_attr24_07102021 PARTITION OF anchor1_attr24 
    FOR VALUES FROM ('2021-10-07 00:00:00') TO ('2021-10-07 23:59:59');
CREATE TABLE anchor1_attr24_06102021 PARTITION OF anchor1_attr24 
    FOR VALUES FROM ('2021-10-06 00:00:00') TO ('2021-10-06 23:59:59');
CREATE TABLE anchor1_attr24_05102021 PARTITION OF anchor1_attr24 
    FOR VALUES FROM ('2021-10-05 00:00:00') TO ('2021-10-05 23:59:59');
CREATE TABLE anchor1_attr24_04102021 PARTITION OF anchor1_attr24 
    FOR VALUES FROM ('2021-10-04 00:00:00') TO ('2021-10-04 23:59:59');
CREATE TABLE anchor1_attr24_08102021 PARTITION OF anchor1_attr24 
    FOR VALUES FROM ('2021-10-08 00:00:00') TO ('2021-10-08 23:59:59');
CREATE INDEX ON anchor1_attr24_07102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr24_06102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr24_05102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr24_04102021 (acnhor1_rk);
CREATE INDEX ON anchor1_attr24_08102021 (acnhor1_rk);



insert into anchor1_attr1
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr2
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr3
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr4
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr5
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr6
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr7
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr8
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr9
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr10
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr11
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr12
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr13
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr14
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr15
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr16
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr17
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr18
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr19
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr20
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr21
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr22
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr23
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
insert into anchor1_attr24
	select acnhor1_rk, random(), cast('2021-10-04 00:00:00' as timestamp),  cast('2021-10-04 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-05 00:00:00' as timestamp), cast('2021-10-05 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-06 00:00:00' as timestamp), cast('2021-10-06 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-07 00:00:00' as timestamp), cast('2021-10-07 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1
	union all
	select acnhor1_rk, random(), cast('2021-10-08 00:00:00' as timestamp), cast('2021-10-08 23:59:59' as timestamp), current_timestamp, etl_id 
	from anchor1;
 	
