
--1.a
--Postgres
--6 attributes (joins)
--without indexes, partitions
--on date (using window function)

do $pr$
DECLARE
  StartTime timestamp;
  EndTime timestamp;
  Delta double precision;
  num integer;
begin
loop 
  num=coalesce(num,0)+1;
  StartTime := clock_timestamp();
  perform 
  from anchor1 a 
  left join anchor1_attr1_v a1 
  	on a.acnhor1_rk=a1.acnhor1_rk
  	and cast('2021-10-07' as date) between a1.from_dttm and a1.to_dttm
left join anchor1_attr2_v a2 
  	on a.acnhor1_rk=a2.acnhor1_rk
  	and cast('2021-10-07' as date) between a2.from_dttm and a2.to_dttm
left join anchor1_attr3_v a3 
  	on a.acnhor1_rk=a3.acnhor1_rk
  	and cast('2021-10-07' as date) between a3.from_dttm and a3.to_dttm
left join anchor1_attr4_v a4 
  	on a.acnhor1_rk=a4.acnhor1_rk
  	and cast('2021-10-07' as date) between a4.from_dttm and a4.to_dttm
left join anchor1_attr5_v a5 
  	on a.acnhor1_rk=a5.acnhor1_rk
  	and cast('2021-10-07' as date) between a5.from_dttm and a5.to_dttm
left join anchor1_attr6_v a6 
  	on a.acnhor1_rk=a6.acnhor1_rk
  	and cast('2021-10-07' as date) between a6.from_dttm and a6.to_dttm
  ; 
  EndTime := clock_timestamp();
    insert into test_result
   select 
   '1a' as test_id, 
   100000 as object_cnt, 
   5 as power,
   StartTime as start_time,
   EndTime as end_time,
   6 as attribute_cnt
   ;
 if num>9 then exit;
	end if; 
end loop; 
END;
$pr$;


--1.a
--Postgres
--12 attributes (joins)
--without indexes, partitions
--on date (using window function)
do $pr$
DECLARE
  StartTime timestamp;
  EndTime timestamp;
  Delta double precision;
  num integer;
begin
loop 
  num=coalesce(num,0)+1;
  StartTime := clock_timestamp();
  perform 
  from anchor1 a 
  left join anchor1_attr1_v a1 
  	on a.acnhor1_rk=a1.acnhor1_rk
  	and cast('2021-10-07' as date) between a1.from_dttm and a1.to_dttm
left join anchor1_attr2_v a2 
  	on a.acnhor1_rk=a2.acnhor1_rk
  	and cast('2021-10-07' as date) between a2.from_dttm and a2.to_dttm
left join anchor1_attr3_v a3 
  	on a.acnhor1_rk=a3.acnhor1_rk
  	and cast('2021-10-07' as date) between a3.from_dttm and a3.to_dttm
left join anchor1_attr4_v a4 
  	on a.acnhor1_rk=a4.acnhor1_rk
  	and cast('2021-10-07' as date) between a4.from_dttm and a4.to_dttm
left join anchor1_attr5_v a5 
  	on a.acnhor1_rk=a5.acnhor1_rk
  	and cast('2021-10-07' as date) between a5.from_dttm and a5.to_dttm
left join anchor1_attr6_v a6 
  	on a.acnhor1_rk=a6.acnhor1_rk
  	and cast('2021-10-07' as date) between a6.from_dttm and a6.to_dttm
left join anchor1_attr7_v a7 
  	on a.acnhor1_rk=a7.acnhor1_rk
  	and cast('2021-10-07' as date) between a7.from_dttm and a7.to_dttm
left join anchor1_attr8_v a8 
  	on a.acnhor1_rk=a8.acnhor1_rk
  	and cast('2021-10-07' as date) between a8.from_dttm and a8.to_dttm
left join anchor1_attr9_v a9 
  	on a.acnhor1_rk=a9.acnhor1_rk
  	and cast('2021-10-07' as date) between a9.from_dttm and a9.to_dttm
left join anchor1_attr10_v a10 
  	on a.acnhor1_rk=a10.acnhor1_rk
  	and cast('2021-10-07' as date) between a10.from_dttm and a10.to_dttm
left join anchor1_attr11_v a11 
  	on a.acnhor1_rk=a11.acnhor1_rk
  	and cast('2021-10-07' as date) between a11.from_dttm and a11.to_dttm
left join anchor1_attr12_v a12 
  	on a.acnhor1_rk=a12.acnhor1_rk
  	and cast('2021-10-07' as date) between a12.from_dttm and a12.to_dttm
  ; 
  EndTime := clock_timestamp();
  insert into test_result
   select 
   '1a' as test_id, 
   100000 as object_cnt, 
   5 as power,
   StartTime as start_time,
   EndTime as end_time,
   12 as attribute_cnt
   ;
 if num>13 then exit;
	end if; 
end loop; 
END;
$pr$;

--1.a
--Postgres
--24 attributes (joins)
--without indexes, partitions
--on date (using window function)
do $pr$
DECLARE
  StartTime timestamp;
  EndTime timestamp;
  Delta double precision;
  num integer;
begin
loop 
  num=coalesce(num,0)+1;
  StartTime := clock_timestamp();
  perform 
  from anchor1 a 
  left join anchor1_attr1_v a1 
  	on a.acnhor1_rk=a1.acnhor1_rk
  	and cast('2021-10-07' as date) between a1.from_dttm and a1.to_dttm
left join anchor1_attr2_v a2 
  	on a.acnhor1_rk=a2.acnhor1_rk
  	and cast('2021-10-07' as date) between a2.from_dttm and a2.to_dttm
left join anchor1_attr3_v a3 
  	on a.acnhor1_rk=a3.acnhor1_rk
  	and cast('2021-10-07' as date) between a3.from_dttm and a3.to_dttm
left join anchor1_attr4_v a4 
  	on a.acnhor1_rk=a4.acnhor1_rk
  	and cast('2021-10-07' as date) between a4.from_dttm and a4.to_dttm
left join anchor1_attr5_v a5 
  	on a.acnhor1_rk=a5.acnhor1_rk
  	and cast('2021-10-07' as date) between a5.from_dttm and a5.to_dttm
left join anchor1_attr6_v a6 
  	on a.acnhor1_rk=a6.acnhor1_rk
  	and cast('2021-10-07' as date) between a6.from_dttm and a6.to_dttm
left join anchor1_attr7_v a7 
  	on a.acnhor1_rk=a7.acnhor1_rk
  	and cast('2021-10-07' as date) between a7.from_dttm and a7.to_dttm
left join anchor1_attr8_v a8 
  	on a.acnhor1_rk=a8.acnhor1_rk
  	and cast('2021-10-07' as date) between a8.from_dttm and a8.to_dttm
left join anchor1_attr9_v a9 
  	on a.acnhor1_rk=a9.acnhor1_rk
  	and cast('2021-10-07' as date) between a9.from_dttm and a9.to_dttm
left join anchor1_attr10_v a10 
  	on a.acnhor1_rk=a10.acnhor1_rk
  	and cast('2021-10-07' as date) between a10.from_dttm and a10.to_dttm
left join anchor1_attr11_v a11 
  	on a.acnhor1_rk=a11.acnhor1_rk
  	and cast('2021-10-07' as date) between a11.from_dttm and a11.to_dttm
left join anchor1_attr12_v a12 
  	on a.acnhor1_rk=a12.acnhor1_rk
  	and cast('2021-10-07' as date) between a12.from_dttm and a12.to_dttm
left join anchor1_attr13_v a13 
  	on a.acnhor1_rk=a13.acnhor1_rk
  	and cast('2021-10-07' as date) between a13.from_dttm and a13.to_dttm
left join anchor1_attr14_v a14 
  	on a.acnhor1_rk=a14.acnhor1_rk
  	and cast('2021-10-07' as date) between a14.from_dttm and a14.to_dttm
left join anchor1_attr15_v a15 
  	on a.acnhor1_rk=a15.acnhor1_rk
  	and cast('2021-10-07' as date) between a15.from_dttm and a15.to_dttm
left join anchor1_attr16_v a16 
  	on a.acnhor1_rk=a16.acnhor1_rk
  	and cast('2021-10-07' as date) between a16.from_dttm and a16.to_dttm
left join anchor1_attr17_v a17 
  	on a.acnhor1_rk=a17.acnhor1_rk
  	and cast('2021-10-07' as date) between a17.from_dttm and a17.to_dttm
left join anchor1_attr18_v a18 
  	on a.acnhor1_rk=a18.acnhor1_rk
  	and cast('2021-10-07' as date) between a18.from_dttm and a18.to_dttm
left join anchor1_attr19_v a19 
  	on a.acnhor1_rk=a19.acnhor1_rk
  	and cast('2021-10-07' as date) between a19.from_dttm and a19.to_dttm
left join anchor1_attr20_v a20 
  	on a.acnhor1_rk=a20.acnhor1_rk
  	and cast('2021-10-07' as date) between a20.from_dttm and a20.to_dttm
left join anchor1_attr21_v a21 
  	on a.acnhor1_rk=a21.acnhor1_rk
  	and cast('2021-10-07' as date) between a21.from_dttm and a21.to_dttm
left join anchor1_attr22_v a22 
  	on a.acnhor1_rk=a22.acnhor1_rk
  	and cast('2021-10-07' as date) between a22.from_dttm and a22.to_dttm
left join anchor1_attr23_v a23 
  	on a.acnhor1_rk=a23.acnhor1_rk
  	and cast('2021-10-07' as date) between a23.from_dttm and a23.to_dttm
left join anchor1_attr24_v a24 
  	on a.acnhor1_rk=a24.acnhor1_rk
  	and cast('2021-10-07' as date) between a24.from_dttm and a24.to_dttm
  ; 
  EndTime := clock_timestamp();
  insert into test_result
   select 
   '1a' as test_id, 
   100000 as object_cnt, 
   5 as power,
   StartTime as start_time,
   EndTime as end_time,
   24 as attribute_cnt
   ;
 if num>15 then exit;
	end if; 
end loop; 
END;
$pr$;

--1.b
--Postgres
--6 attributes (joins)
--without indexes, partitions
--max date (using max table)
do $pr$
DECLARE
  StartTime timestamp;
  EndTime timestamp;
  Delta double precision;
  num integer;
begin
loop 
  num=coalesce(num,0)+1;
  StartTime := clock_timestamp();
  perform 
  from anchor1 a 
  left join anchor1_attr1_lv a1 
  	on a.acnhor1_rk=a1.acnhor1_rk
left join anchor1_attr2_lv a2 
  	on a.acnhor1_rk=a2.acnhor1_rk
left join anchor1_attr3_lv a3 
  	on a.acnhor1_rk=a3.acnhor1_rk
left join anchor1_attr4_lv a4 
  	on a.acnhor1_rk=a4.acnhor1_rk
left join anchor1_attr5_lv a5 
  	on a.acnhor1_rk=a5.acnhor1_rk
left join anchor1_attr6_lv a6 
  	on a.acnhor1_rk=a6.acnhor1_rk
  ; 
  EndTime := clock_timestamp();
    insert into test_result
   select 
   '1b' as test_id, 
   100000 as object_cnt, 
   5 as power,
   StartTime as start_time,
   EndTime as end_time,
   6 as attribute_cnt
   ;
 if num>9 then exit;
	end if; 
end loop; 
END;
$pr$;

--1.b
--Postgres
--12 attributes (joins)
--without indexes, partitions
--max date (using max table)
do $pr$
DECLARE
  StartTime timestamp;
  EndTime timestamp;
  Delta double precision;
  num integer;
begin
loop 
  num=coalesce(num,0)+1;
  StartTime := clock_timestamp();
  perform 
  from anchor1 a 
  left join anchor1_attr1_lv a1 
  	on a.acnhor1_rk=a1.acnhor1_rk
left join anchor1_attr2_lv a2 
  	on a.acnhor1_rk=a2.acnhor1_rk
left join anchor1_attr3_lv a3 
  	on a.acnhor1_rk=a3.acnhor1_rk
left join anchor1_attr4_lv a4 
  	on a.acnhor1_rk=a4.acnhor1_rk
left join anchor1_attr5_lv a5 
  	on a.acnhor1_rk=a5.acnhor1_rk
left join anchor1_attr6_lv a6 
  	on a.acnhor1_rk=a6.acnhor1_rk
 left join anchor1_attr7_lv a7
  	on a.acnhor1_rk=a7.acnhor1_rk
left join anchor1_attr8_lv a8
  	on a.acnhor1_rk=a8.acnhor1_rk
left join anchor1_attr9_lv a9
  	on a.acnhor1_rk=a9.acnhor1_rk
left join anchor1_attr10_lv a10
  	on a.acnhor1_rk=a10.acnhor1_rk
left join anchor1_attr11_lv a11
  	on a.acnhor1_rk=a11.acnhor1_rk
left join anchor1_attr12_lv a12
  	on a.acnhor1_rk=a12.acnhor1_rk
  ; 
  EndTime := clock_timestamp();
    insert into test_result
   select 
   '1b' as test_id, 
   100000 as object_cnt, 
   5 as power,
   StartTime as start_time,
   EndTime as end_time,
   12 as attribute_cnt
   ;
 if num>9 then exit;
	end if; 
end loop; 
END;
$pr$;


--1.b
--Postgres
--24 attributes (joins)
--without indexes, partitions
--max date (using max table)
do $pr$
DECLARE
  StartTime timestamp;
  EndTime timestamp;
  Delta double precision;
  num integer;
begin
loop 
  num=coalesce(num,0)+1;
  StartTime := clock_timestamp();
  perform 
  from anchor1 a 
  left join anchor1_attr1_lv a1 
  	on a.acnhor1_rk=a1.acnhor1_rk
left join anchor1_attr2_lv a2 
  	on a.acnhor1_rk=a2.acnhor1_rk
left join anchor1_attr3_lv a3 
  	on a.acnhor1_rk=a3.acnhor1_rk
left join anchor1_attr4_lv a4 
  	on a.acnhor1_rk=a4.acnhor1_rk
left join anchor1_attr5_lv a5 
  	on a.acnhor1_rk=a5.acnhor1_rk
left join anchor1_attr6_lv a6 
  	on a.acnhor1_rk=a6.acnhor1_rk
 left join anchor1_attr7_lv a7
  	on a.acnhor1_rk=a7.acnhor1_rk
left join anchor1_attr8_lv a8
  	on a.acnhor1_rk=a8.acnhor1_rk
left join anchor1_attr9_lv a9
  	on a.acnhor1_rk=a9.acnhor1_rk
left join anchor1_attr10_lv a10
  	on a.acnhor1_rk=a10.acnhor1_rk
left join anchor1_attr11_lv a11
  	on a.acnhor1_rk=a11.acnhor1_rk
left join anchor1_attr12_lv a12
  	on a.acnhor1_rk=a12.acnhor1_rk
left join anchor1_attr13_lv a13
  	on a.acnhor1_rk=a13.acnhor1_rk
left join anchor1_attr14_lv a14
  	on a.acnhor1_rk=a14.acnhor1_rk
left join anchor1_attr15_lv a15
  	on a.acnhor1_rk=a15.acnhor1_rk
left join anchor1_attr16_lv a16
  	on a.acnhor1_rk=a16.acnhor1_rk
left join anchor1_attr17_lv a17
  	on a.acnhor1_rk=a17.acnhor1_rk
left join anchor1_attr18_lv a18
  	on a.acnhor1_rk=a18.acnhor1_rk
left join anchor1_attr19_lv a19
  	on a.acnhor1_rk=a19.acnhor1_rk
left join anchor1_attr20_lv a20
  	on a.acnhor1_rk=a20.acnhor1_rk
left join anchor1_attr21_lv a21
  	on a.acnhor1_rk=a21.acnhor1_rk
left join anchor1_attr22_lv a22
  	on a.acnhor1_rk=a22.acnhor1_rk
left join anchor1_attr23_lv a23
  	on a.acnhor1_rk=a23.acnhor1_rk
left join anchor1_attr24_lv a24
  	on a.acnhor1_rk=a24.acnhor1_rk
  ; 
  EndTime := clock_timestamp();
    insert into test_result
   select 
   '1b' as test_id, 
   100000 as object_cnt, 
   5 as power,
   StartTime as start_time,
   EndTime as end_time,
   24 as attribute_cnt
   ;
 if num>9 then exit;
	end if; 
end loop; 
END;
$pr$;

