from database import Postgres
import pandas as pd
import datetime as dt
import json


KEYWORD = ['Split', 'Cycle', 'Setlamp', ' Coordination','Plan','Activated','Remove']
TIME_PERIOD = 5
MATCH_LIMITTIME = 30
# pg_inf_46 = {'database': "signal_specialist", 'user': "postgres", 'password': "postgres",
#           'host': "192.168.20.46", 'port': "5432"}
pg_inf_46 = {'database': "signal_specialist", 'user': "django", 'password': "postgres",
          'host': "33.83.100.145", 'port': "5432"}



sql_insert = "insert into operate_descripe values(%s,%s,%s,%s) "

sql_operate_static = " SELECT userid,time_serice,to_timestamp(operdate||' '||to_char(time_serice*{0}/60,'99')||':'||" \
                     "to_char(mod(time_serice*{0},60), '99')||':00','yyyy-MM-dd hh24:mi:ss') as datetime," \
                     "count(*),string_agg(all_type,',')as all_type FROM(select distinct userid, opertime," \
                     "string_agg(opertype,',')as all_type,siteid,(extract(hour from opertime)::int*60 + " \
                     "extract(minute from opertime)::int)/{0} as time_serice ,to_char(opertime,'yyyy-mm-dd')" \
                     "as operdate from record_data_parsing where opertype is not null and  opertype != '' " \
                     "and to_char(opertime,'yyyy-mm-dd') = '{1}' GROUP BY userid,siteid,opertime " \
                     "order by userid,siteid,opertime)a GROUP BY userid,time_serice,operdate " \
                     "order by userid,time_serice"

sql_distinct = "select distinct userid, opertime,string_agg(opertype,',')as all_type,siteid," \
      "(extract(hour from opertime)::int*60 + extract(minute from opertime)::int)/5 as time_serice ," \
      "to_char(opertime,'yyyy-mm-dd')as operdate from record_data_parsing " \
      "where opertype is not null and  opertype != '' and to_char(opertime,'yyyy-MM-dd') = '{0}' " \
      "GROUP BY userid,siteid,opertime order by userid,siteid,opertime"

sql_get_alarm_data = "select a.inter_id,a.inter_name,a.time_point,a.alarm_id,a.delay,d.systemid,d.subsystem_id " \
                     "from disposal_alarm_data a,(select b.*,c.subsystem_id from( SELECT a.gaode_id, a.gaode_name, " \
                     "a.inter_id,a.inter_name,b.systemid FROM (gaode_inter_rel a LEFT JOIN pe_tobj_node b " \
                     "ON (((a.inter_id)::text = (b.nodeid)::text)))) b,subid_scatsid_relationship c " \
                     "where b.systemid = c.site_id)d where a.inter_id = d.gaode_id and a.time_point " \
                     "BETWEEN '{0}' and '{1}'"

sql_alarm_operate_match = """
insert into alarm_operate_match SELECT a.userid,a.opertime,a.all_type,a.siteid,b.scats_id ,b.inter_id,b.inter_name,b.time_point,b.alarm_id
FROM
(
select a.*,substring(a.siteid from 'SS=#"%#"' FOR '#') as subsystem_id,
substring(a.siteid from 'I=#"%#"' FOR '#') as scats_id 
from 
(
select distinct userid, opertime,siteid,string_agg(opertype,',') as all_type,
to_char(opertime,'yyyy-mm-dd')as operdate 
from record_data_parsing 
where opertype is not null and  opertype not in ('Activate','Remove') and opertime BETWEEN '{1}' and '{2}'
group by userid, opertime,siteid
order by userid,siteid,opertime
)a
)a ,
(
select a.inter_id,a.inter_name,a.time_point,a.alarm_id,a.delay,d.scats_id,d.subsystem_id 
from disposal_alarm_data a
,
(
select b.*,c.subsystem_id
from
(
 SELECT a.gaode_id,
    a.gaode_name,
    a.inter_id,
    a.inter_name,
    b.scats_id
   FROM (gaode_inter_rel a
     LEFT JOIN 
(
SELECT
		*
	FROM
		dblink (
			'host={0} dbname=inter_info user=postgres password=postgres',
			'select sys_code, node_id from pe_tobj_node_info'
		) AS T (
			scats_id VARCHAR (15),
			nodeid VARCHAR (50)
		)
)b ON (((a.inter_id)::text = (b.nodeid)::text)))
)b left join subid_scatsid_relationship c
on b.scats_id = c.site_id
)d
where a.inter_id = d.gaode_id
and a.time_point BETWEEN '{1}' and '{2}'
) b
where (a.subsystem_id = b.subsystem_id or a.scats_id = b.scats_id)
and a.opertime BETWEEN b.time_point and (b.time_point + '{3}minutes'::INTERVAL)
"""


class Operater():

    def __init__(self):
        self.count_dict = {}
        for key in KEYWORD:
            self.count_dict[key] = 0


class OperateStatistics():
    def __init__(self, operate_data=pd.DataFrame({})):
        self.operate_data = operate_data
        self.count_dict = {}
        for key in KEYWORD:
            self.count_dict[key] = 0

    def operate_type_percent(self):
        operate_data = self.operate_data
        for i in range(len(operate_data)):
            oper_type = operate_data.iloc[i][6]
            for type in oper_type:
                count = self.count_dict.get(type)
                if count is not None:
                    self.count_dict[type] += 1
        return self.count_dict

    def user_operate_sum(self):
        operate_data = self.operate_data
        grouped = operate_data.groupby('userid')
        result = {}
        # operate_data.drop_duplicates()
        for index, group in grouped:
            operater = Operater()
            sum_num = len(group['opertime'].drop_duplicates().tolist())
            result[index] = {'OperNum':sum_num}
            for i in range(len(group)):
                oper_type = group.iloc[i][6]
                for type in oper_type:
                    count = operater.count_dict.get(type)
                    if count is not None:
                        operater.count_dict[type] += 1
                result[index]['TypeCount'] = operater.count_dict
        return result

    def alarm_operate_match(self, s_datetime, e_datetime):
        pg_inf = pg_inf_46
        self.pg = Postgres(pg_inf)
        host = pg_inf.get('host')

        conn,cr = self.pg.db_conn()
        if cr:
            cr.execute(sql_alarm_operate_match.format(host,s_datetime,e_datetime,MATCH_LIMITTIME))
            conn.commit()


    # def get_operate_time_static(self):
    #     conn,cr = self.pg.db_conn()
    #     cr.execute(sql_operate_static)
    #     rows = cr.fetchall()
    #     if len(rows) > 0:

def operate_data_send(operate_data):
    send_data = []
    for key in operate_data.keys():
        describe = operate_data.get(key)
        if describe is not None:
            datetime = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')
            oper_num = describe.get('OperNum')
            json_describe = json.dumps(describe, ensure_ascii=False)
            send_data.append([datetime,json_describe,key,oper_num])
    pg = Postgres()
    conn, cr = pg.db_conn()
    for data in send_data:
        print(data)
        cr.execute(sql_insert, data)
        conn.commit()
    pg.db_close()


def tuple2frame(result, index):
        column_name = []
        for i in range(len(index)):
            index_name = index[i][0]
            column_name.append(index_name)
        result = pd.DataFrame(result, columns=column_name)
        return result


def call_pg_data(sql,pg_inf=None):
    if pg_inf:
        db = Postgres(pg_inf)
    else:
        db = Postgres()
    pg_conn, cr = db.db_conn()
    if pg_conn:
        cr.execute(sql)
        index = cr.description
        result = cr.fetchall()
        db.db_close()

        if result:
            fresult = tuple2frame(result, index)
            return fresult
        else:
            return pd.DataFrame({})


def get_operate_data():
    def oper_type_drop_duplicate(operate_data):
        operate_data['type'] = operate_data['all_type'].apply(lambda x: sorted(x.split(',')) if x else None)
        # data['date'] = data['time_point'].apply(lambda x: x.date() if x else None)
        return operate_data

    current_date = dt.datetime.now().date()
    current_date = '2018-10-15'
    operate_data = call_pg_data(sql_distinct.format(current_date))
    operate_time_static = call_pg_data(sql_operate_static.format(TIME_PERIOD,current_date))

    if len(operate_data) > 0:
        operate_data = oper_type_drop_duplicate(operate_data)
        print(operate_data)
        O1 = OperateStatistics(operate_data)
        result = O1.operate_type_percent()
        result2 = O1.user_operate_sum()
        print(result2)
        # operate_data_send(result2)
    return result2

def get_alarm_data():
    # pg_inf = {'database': "research", 'user': "postgres", 'password': "postgres",
    #                     'host': "192.168.20.45",'port': "5432"}
    pg_inf = pg_inf_46
    current_date = dt.datetime.now().date()
    current_date = '2018-10-15'
    start_data = current_date+' 00:00:00'
    end_data = current_date + ' 23:59:59'
    alarm_data = call_pg_data(sql_get_alarm_data.format(start_data, end_data),pg_inf)
    print(alarm_data)
    return alarm_data


if __name__ == '__main__':
    O1 = OperateStatistics()
    s_datetime = '2018-10-15 00:00:00'
    e_datetime = '2018-10-15 23:59:59'
    O1.alarm_operate_match(s_datetime,e_datetime)
    # get_alarm_data()
    # get_operate_data()