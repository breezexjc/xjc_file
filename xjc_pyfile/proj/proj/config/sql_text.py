
class SqlText():
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