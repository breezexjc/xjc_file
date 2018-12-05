table_salk_list ="HZ_SCATS_OUTPUT"
man_operate = "GET_MANOPERATION_RECORD"

sql_get_salklist_status = """
select count(*) from HZ_SCATS_OUTPUT where fstr_date =to_date(: a,'yyyy-MM-dd')  and 
FSTR_CYCLE_STARTTIME between :b and :f
"""
#
sql_get_opetate_status = """
select count(*) from GET_MANOPERATION_RECORD where OPERTIME between :a and :b
"""

sql_failed_detector = """
select distinct FSTR_INTERSECTID,FINT_SA,FINT_DETECTORID  from HZ_SCATS_OUTPUT where fstr_date =to_date(: a,'yyyy-MM-dd')
and FINT_DETECTORID> :b
"""

sql_send_message = """
insert into interface_status_check(interface_name,insert_time,insert_num,exception) values(%s,%s,%s,%s)
"""

sql_send_parse_failed_detector = """
insert into parse_failed_detector_list(scats_id,salk_no,detector) values(%s,%s,%s)
"""
# sql3 = "select count(*) from RUN_STR_INFORMATION_SALKLIST1 WHERE RECVTIME BETWEEN {0} AND {1}".format(
#     starttime_time, endtime_time)
# sql4 = "select count(*) from GET_MANOPERATE_RECORD WHERE OPERTIME BETWEEN {0} AND {1}".format(
#     starttime_time,
#     endtime_time)