
class SqlText():
    pg_inf_arith = {'database': "arithmetic", 'user': "postgres", 'password': "postgres",
                    'host': "192.168.20.46", 'port': "5432"}

    sql_operate_match = """
SELECT aom.*,acc.user_name,
(case when oper_type='Split' then '优化绿信比' 
when oper_type='Cycle' then '优化周期方案' 
when oper_type='Dwell' then '路口定灯'
when oper_type='Coordination' then '优化协调方案' 
when oper_type='PP' then '修改内部方案'
when oper_type='XSF' then '修改标志位'
when oper_type='Other' then '修改其他内容'
end)as oper_desc
FROM
(
	SELECT
		userid,
		oper_time,
		oper,
		oper_type
	FROM
		alarm_operate_match
	WHERE
		inter_id = '{0}'
	AND oper_time > to_timestamp('{1}', 'yyyy-MM-dd')
) aom
LEFT JOIN (
	SELECT
		user_name,company_id
	FROM
		sms_user
) acc
on aom.userid = acc.company_id
order by aom.oper_time desc
"""
    sql_getscats_operate = """
        SELECT
    	aom.*, acc.user_name,
    	(
    		CASE
    		WHEN aom.oper_type = 'Split' THEN
    			'优化绿信比'
    		WHEN aom.oper_type = 'Cycle' THEN
    			'优化周期方案'
    		WHEN aom.oper_type = 'Dwell' THEN
    			'路口定灯'
    		WHEN aom.oper_type = 'Coordination' THEN
    			'优化协调方案'
    		WHEN aom.oper_type = 'PP' THEN
    			'修改内部方案'
    		WHEN aom.oper_type = 'XSF' THEN
    			'修改标志位'
    		WHEN aom.oper_type = 'Other' THEN
    			'修改其他内容'
    		END
    	) AS oper_desc
    FROM
    	(
    		SELECT
    			userid,
    			opertime,
    			oper,
    			all_type as oper_type
    		FROM
    			(
    				SELECT
    					A .userid,
    					A .opertime,
    					A .oper,
    					A .meaning,
    					A .all_type,
    					A .siteid,
    					(
    						CASE
    						WHEN A .scats_id IS NOT NULL THEN
    							A .scats_id
    						ELSE
    							C .site_id
    						END
    					) AS scats_id
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
    								AND opertype NOT IN ('Activated', 'Remove')
    								AND opertime BETWEEN '{1}'
    								AND '{2}'
    							) A
    					) A
    				LEFT JOIN subid_scatsid_relationship C ON A .subsystem_id = C .subsystem_id
    			) T
    		WHERE
    			scats_id = '{0}'
    		GROUP BY
    			userid,
    			opertime,
    			scats_id,
    			siteid,
    			all_type,
    			oper,
    			meaning
    	) aom

    LEFT JOIN (
    	SELECT
    		user_name,
    		company_id
    	FROM
    		sms_user
    ) acc ON aom.userid = acc.company_id
    order by aom.opertime desc
        """
    sql_get_spilt_number="""select split ,count(*)  as spilt_number from scats_int_state_feedback group by split"""
    sql_get_cycle_number="""select cycle ,count(*)  as cycle_number from scats_int_state_feedback group by cycle"""
    sql_get_interface_status = """
          select * from interface_status_check where record_time between '{0}' and '{1}'
          """
    sql_get_parse_failed_detector = """
          select * from parse_failed_detector_list where record_time = '{0}'
          """

    sql_delete_real_phase = """delete from st_realtime_phase_inf where pushtime < '{0}'"""
    sql_delete_kde_vaue = """delete from disposal_alarm_data_kde_value where time_point <'{0}'"""

    sql_sche_check = """
    SELECT
        dad."name",dadj.status,dad.next_run_time,dadj.duration,dadj."exception",dadj.traceback
    FROM
        django_apscheduler_djangojob dad
    LEFT JOIN 
    (select a.job_id,a.run_time,a.duration,a.status,a."exception",a.traceback
     from django_apscheduler_djangojobexecution_copy a
    left JOIN(
    select job_id,max(run_time) as run_time from
    django_apscheduler_djangojobexecution
    GROUP BY job_id) b
    on a.job_id=b.job_id and a.run_time=b.run_time
    ) dadj ON dad."id" = dadj.job_id
    """