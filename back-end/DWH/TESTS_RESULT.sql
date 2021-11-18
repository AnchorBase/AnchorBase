
create table test_result 
	(
		test_id varchar(10),
		object_cnt integer,
		power integer,
		start_time timestamp,
		end_time timestamp,
		duration double precision,
		attribute_cnt integer
		);

select test_id, object_cnt, power, attribute_cnt,
		sum(
			 (extract(hours from end_time) - extract(hours from start_time))*3600
  			+(extract(seconds from end_time) - extract(seconds from start_time))
  			+(extract(minutes from end_time) - extract(minutes from start_time))*60
  			)/count(*) as duration_avg,
		count(*) as test_cnt
		from test_result
		group by test_id, object_cnt, power, attribute_cnt

		
