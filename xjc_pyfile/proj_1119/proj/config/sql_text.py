



class SqlText():

    pg_inf_arith = {'database': "arithmetic", 'user': "postgres", 'password': "postgres",
                    'host': "192.168.20.46", 'port': "5432"}
    pg_inf_django = {'database': "django", 'user': "django", 'password': "postgres",
                    'host': "192.168.20.45", 'port': "5432"}
    """操作记录匹配"""
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
"""
    """获取scats操作记录"""
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
								AND opertype NOT IN ('Activate', 'Remove')
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
    """获取路口方案锁定信息"""
    sql_get_splitlocked = """
    SELECT
	A .siteid,
	B .splitplanlocked
  FROM
	(
		SELECT
			siteid,
			max(pushtime) as pushtime
		FROM
			st_realtime_phase_inf
		GROUP BY siteid
	) A,
st_realtime_phase_inf B
where A.siteid=B.siteid and A.pushtime = B.pushtime
    """
    """删除实时相位历史数据"""
    sql_delete_real_phase = """delete from st_realtime_phase_inf where pushtime < '{0}'"""
    """删除历史kde强度数据"""
    sql_delete_kde_vaue = """delete from disposal_alarm_data_kde_value where time_point <'{0}'"""
    """获取接口数据获取状态数据"""
    sql_get_interface_status = """
        select * from interface_status_check where record_time between '{0}' and '{1}' order by record_time
        """
    """获取解析失败检测器编号"""
    sql_get_parse_failed_detector = """
        select * from parse_failed_detector_list where record_time = '{0}'
        """
    """获取定时任务运行状态"""
    sql_sche_check = """
       SELECT
           dad."name",dadj.status,dadj.run_time,dadj.duration,dadj."exception",dadj.traceback
       FROM
           django_apscheduler_djangojob dad
       LEFT JOIN 
       (select a.job_id,a.run_time,a.duration,a.status,a."exception",a.traceback
        from django_apscheduler_djangojobexecution a
       right JOIN(
       select job_id,max(run_time) as run_time from
       django_apscheduler_djangojobexecution
       GROUP BY job_id) b
       on a.job_id=b.job_id and a.run_time=b.run_time
       ) dadj ON dad."id" = dadj.job_id
       """
    """获取车道饱和状态"""
    sql_get_lane_status = """
     SELECT E.FSTR_INTERSECTID,E.FINT_DETECTORID,E.FSTR_DATE,E.FSTR_CYCLE_STARTTIME,E.FINT_CYCLE_LENGTH,
  E.REAL_CAP AS MLC, E.FREE_CAP AS RLC,E.REAL_VO AS OLC,e.MAX_CAP AS BLC,
  E.FINT_ACTUALVOLUME AS FINT_ACTUALVOLUME,(CASE WHEN E.MAX_CAP !=0 THEN 
  ROUND((100.00-(100*E.REAL_CAP/E.MAX_CAP)),2)END)AS CLR,
  (CASE WHEN E.REAL_CAP !=0 THEN ROUND((100*E.REAL_VO/E.REAL_CAP),2)END)AS CUR,E.FINT_DS
  FROM
  (
  SELECT C.FSTR_INTERSECTID,C.FSTR_DATE,C.FSTR_CYCLE_STARTTIME,C.FINT_CYCLE_LENGTH,C.FINT_DETECTORID,
  C.FINT_ACTUALVOLUME,C.REAL_CAP,C.FREE_CAP,C.REAL_VO,C.MAX_CAP,c.FINT_DS
  FROM(
  SELECT a.*,(case when a.FINT_DS<=b.DSM then ROUND((A.FINT_PHASE_LENGTH*B.MAX_CAP/A.FINT_CYCLE_LENGTH),1) 
  when a.FINT_DS>b.DSM then ROUND((A.FINT_PHASE_LENGTH*(B.FIT_BUSY1+B.FIT_BUSY2*A.FINT_DS)/A.FINT_CYCLE_LENGTH),1) end) as REAL_CAP,
  (case when a.FINT_DS<=b.DSM then ROUND((A.FINT_PHASE_LENGTH*(B.MAX_CAP-B.FIT_FREE*A.FINT_DS)/A.FINT_CYCLE_LENGTH),1)
  when a.FINT_DS>b.DSM then 0 end) as FREE_CAP,
   (case when a.FINT_DS<=b.DSM then ROUND((A.FINT_PHASE_LENGTH*(B.FIT_FREE*A.FINT_DS)/A.FINT_CYCLE_LENGTH),1)
  when a.FINT_DS>b.DSM then ROUND((A.FINT_PHASE_LENGTH*(B.FIT_BUSY1+B.FIT_BUSY2*A.FINT_DS)/A.FINT_CYCLE_LENGTH),1) end) as REAL_VO,
  ROUND((A.FINT_PHASE_LENGTH*B.MAX_CAP/A.FINT_CYCLE_LENGTH),1) as max_cap
  FROM (
  select c.FSTR_INTERSECTID,c.FINT_DETECTORID,c.FSTR_CYCLE_STARTTIME,c.FINT_PHASE_LENGTH,c.FINT_CYCLE_LENGTH,c.FINT_DS,c.FINT_ACTUALVOLUME,c.FSTR_DATE 
  from enjoyor.HZ_SCATS_OUTPUT c right join(
  select a.FSTR_DATE,a.FSTR_INTERSECTID,max(a.fstr_cycle_starttime) as fstr_cycle_starttime  
  from enjoyor.HZ_SCATS_OUTPUT a ,
   (
  select max(FSTR_DATE) as FSTR_DATE  from 
  enjoyor.HZ_SCATS_OUTPUT) b 
  where a.FSTR_DATE=b.FSTR_DATE and a.fstr_cycle_starttime<'23:50:00'
  group by a.FSTR_DATE,a.FSTR_INTERSECTID
  ) d
  on c.FSTR_DATE=d.FSTR_DATE and c.fstr_cycle_starttime=d.fstr_cycle_starttime and c.FSTR_INTERSECTID = d.FSTR_INTERSECTID
  )a
  ,
  HZ_ROSECT_MODEL_TP b
  where a.FSTR_INTERSECTID=b.INT and a.FINT_DETECTORID=b.LANENO and a.FINT_CYCLE_LENGTH !=0 
  )C
  where C.MAX_CAP >0
   )E
    """
    """获取接口请求记录"""
    sql_get_request_manage = """
    select * from app_manage_interfacerequestrecord where request_time > current_date
    """