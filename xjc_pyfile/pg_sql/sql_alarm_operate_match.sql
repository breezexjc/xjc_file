CREATE OR REPLACE FUNCTION "public"."sql_alarm_operate_match"("stime" timestamp, "etime" timestamp, "time_interval" varchar='10minutes'::character varying)
  RETURNS "pg_catalog"."varchar" AS $BODY$BEGIN
	--Routine body goes here...
insert into alarm_operate_match(userid,oper_time,scats_id,oper,meaning,oper_type,siteid,inter_id,inter_name,alarm_time,alarm_id)
SELECT
	d.*, e.alarm_id
FROM
	(
		SELECT
			userid,
			opertime,
			scats_id,
			oper,
			meaning,
			all_type,
			siteid,
			inter_id,
			inter_name,
			MAX (time_point) AS alarm_time 
   
		FROM
			(
				SELECT
					A .userid,
					A .opertime,
					A .oper,
					A .meaning,
					A .all_type,
					A .siteid,
					b.scats_id AS scats_id,
					b.inter_id,
					b.inter_name,
					b.time_point,
					b.alarm_id
				FROM
					(
						SELECT
							A .*, SUBSTRING (
								A .siteid
								FROM
									'SS=#"%#"' FOR '#'
							) AS subsystem_id,
							SUBSTRING (
								A .siteid
								FROM
									'I=#"%#"' FOR '#'
							) AS scats_id
						FROM
							(
								SELECT DISTINCT
									oper,
									meaning,
									userid,
									opertime,
									opertype AS all_type,
									siteid,
									to_char(opertime, 'yyyy-mm-dd') AS operdate
								FROM
									record_data_parsing
								WHERE
 						    siteid IS NOT NULL 								
								AND opertype NOT IN ('Activate', 'Remove')
								 and opertime between stime and etime
								ORDER BY
									userid,
									siteid,
									opertime
							) A
					) A
				left JOIN (
					SELECT
						A .inter_id,
						A .inter_name,
						A .time_point,
						A .alarm_id,
						A .delay,
						d.scats_id,
						d.subsystem_id
					FROM
						disposal_alarm_data A,
						(
							SELECT
								b.*, C .subsystem_id
							FROM
								(
									SELECT
										A .gaode_id,
										A .gaode_name,
										A .inter_id,
										A .inter_name,
										b.scats_id
									FROM
										(
											gaode_inter_rel A
											LEFT JOIN (
												SELECT
													*
												FROM
													dblink (
														'host=192.168.20.46 dbname=inter_info user=postgres password=postgres',
														'select sys_code, node_id from pe_tobj_node_info'
													) AS T (
														scats_id VARCHAR (15),
														nodeid VARCHAR (50)
													)
											) b ON (
												(
													(A .inter_id) :: TEXT = (b.nodeid) :: TEXT
												)
											)
										)
								) b
							LEFT JOIN subid_scatsid_relationship C ON b.scats_id = C .site_id
						) d
					WHERE A .inter_id = d.gaode_id
					AND A .time_point between stime and etime
				) b ON (
					A .subsystem_id = b.subsystem_id
					OR A .scats_id = b.scats_id
				)
				AND A .opertime BETWEEN b.time_point
				AND (
					b.time_point + time_interval :: INTERVAL
				)
			) T
		GROUP BY
			userid,
			opertime,
			scats_id,
			siteid,
			inter_name,
			all_type,
			inter_id,
			oper,
			meaning
	) d
inner JOIN (
	SELECT DISTINCT
		A .alarm_id,
		A .inter_id,
		A .time_point
	FROM
		disposal_alarm_data A
	WHERE
		A .time_point between stime and etime
) e ON d.inter_id = e.inter_id
AND d.alarm_time = e.time_point 
LEFT JOIN (
	SELECT
		disp_id,
		alarm_id
	FROM
		disposal_data
	WHERE
		date_day + date_time between stime and etime
)f
on e.alarm_id = f.alarm_id
ORDER BY
	userid,
	opertime,
	scats_id;
RETURN 'success!';
END
$BODY$
  LANGUAGE 'plpgsql' VOLATILE COST 100
;

ALTER FUNCTION "public"."sql_alarm_operate_match"("stime" timestamp, "etime" timestamp, "time_interval" varchar) OWNER TO "postgres";