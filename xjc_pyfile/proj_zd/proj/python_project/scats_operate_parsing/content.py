from datetime import timedelta,date,datetime
import datetime

class CONTENT:
    endtime = datetime.datetime.now()
    starttime = endtime - timedelta(minutes=10)
    #选取所有路口SITEID
    sql1= "select SITEID from INTERSECT_INFORMATION"
    #读取人工操作记录，固定时间，本地测试用
    sql2="select * from record_data_parsing where  OPERTIME BETWEEN '2018-10-19' AND '2018-10-20'"
    #从已解析玩的数据中读取数据，准备进行状态分析,固定时间，本地测试用
    sql3="select * from record_data_parsing where opertime between '2018-10-15' and '2018-10-16'"
    #将解析后的人工操作记录插入数据库
    sql4= "insert into record_data_parsing(oper,meaning,opercode,opertime,opertype,region,siteid,userid,message) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #读取原始的人工操作记录
    #sql5="select * from GET_MANOPERATION_RECORD where  OPERTIME BETWEEN '{0}' AND '{1}'".format(starttime,endtime)
    # sql_get_manoperate = "select * from GET_MANOPERATION_RECORD_HIS2 where  OPERTIME BETWEEN" \
    #                      " to_timestamp('{0}','yyyy-MM-dd HH24:mi:ss') AND to_timestamp('{1}','yyyy-MM-dd HH24:mi:ss')"
    sql_get_manoperate = "select * from GET_MANOPERATION_RECORD where  OPERTIME BETWEEN" \
                         " '{0}' AND '{1}'"
    #从已解析的数据中读取数据，准备进行状态分析
    sql6="select * from record_data_parsing where opertime between '{0}' and '{1}'".format(starttime,endtime)
    #从表格中读取子系统（SS）与路口（I）的关系
    #sql7="select site_id from subid_scatsid_relationship where subsystem_id='{0}'".format(scats_id)
    #从表格中读取USER_ID与操作人员姓名的关系
    #sql8="select * from scats_user_accounts where userid='{0}'".format(user_id)
    #删除原状态信息的表格
    sql9="delete from scats_int_state_feedback"
    #将最新的状态信息表插入
    sql10="insert into scats_int_state_feedback(updatetime,user_name,split,cycle,coordination,pp,dwell,xsf,other" \
          ",siteid) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
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

    sql_operate_match = """
    SELECT
	A .userid,a.opertime,a.opertype,a.message,a.siteid,
	(
		CASE
		WHEN A .scats_id IS NULL THEN
			C .site_id
		ELSE
			A .scats_id
		END
	) AS scats_id,
	(CASE WHEN d.user_name IS NULL then '其他单位' else d.user_name end) as user_name
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
					userid,
					opertime,
					opertype,
					siteid,
					message
				FROM
					record_data_parsing
				WHERE
					opertype IS NOT NULL
				AND siteid IS NOT NULL
				AND opertype NOT IN ('Activate')
				AND opertime BETWEEN '{0}'
				AND '{1}'
				ORDER BY
					userid,
					siteid,
					opertime
			) A
	) A
LEFT JOIN subid_scatsid_relationship C ON C .subsystem_id = A .subsystem_id
LEFT JOIN sms_user d ON A .userid = d.company_id
"""

    sql_init_cycle = "select siteid,cycle from scats_int_state_feedback "