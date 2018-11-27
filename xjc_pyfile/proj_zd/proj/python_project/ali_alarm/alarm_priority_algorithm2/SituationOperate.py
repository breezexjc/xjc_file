import psycopg2
from proj.config.database import Postgres

import pandas as pd
import time
import datetime as dt
SO_INTERVAL = '15minutes'

import logging
logger2 = logging.getLogger('sourceDns.webdns.views')  # 获取settings.py配置文件中logger名称


# pg_inf = {'database': "signal_specialist", 'user': "django", 'password': "postgres",
#                'host': "192.168.20.46", 'port': "5432"}

pg_inf = {'database': "signal_specialist", 'user': "django", 'password': "postgres",
               'host': "33.83.100.145", 'port': "5432"}

IFTEST = True

class SituationOperate():

    sql = "select * from record_data_resolve where opertime between '{0}' and '{1}'"
    sql_operate_statistic = r"""
    select a.*,substring(a.siteid from 'SS=#"%#"' FOR '#') as subsystem_id,
    substring(a.siteid from 'I=#"%#"' FOR '#') as scats_id 
    from 
    (
    select distinct userid, opertime,siteid,string_agg(opertype,',') as all_type,
    to_char(opertime,'yyyy-mm-dd')as operdate 
    from record_data_parsing 
    where opertype is not null and  opertype not in ('Activate','Remove') and opertime between
		current_timestamp-'1hour'::INTERVAL and current_timestamp
    group by userid, opertime,siteid
    order by userid,siteid,opertime
    )a;
    """
    sql_operate_statistic_test = r"""
        select a.*,substring(a.siteid from 'SS=#"%#"' FOR '#') as subsystem_id,
        substring(a.siteid from 'I=#"%#"' FOR '#') as scats_id 
        from 
        (
        select distinct userid, opertime,siteid,string_agg(opertype,',') as all_type,
        to_char(opertime,'yyyy-mm-dd')as operdate 
        from record_data_parsing 
        where opertype is not null and  opertype not in ('Activate','Remove') and 
        to_char(opertime,'yyyy-mm-dd') = '2018-10-15'
        group by userid, opertime,siteid
        order by userid,siteid,opertime
        )a;
      """
    sql_alarm_operate_match = 'sql_alarm_operate_match'

    sql_subsys_inf = "select * from subid_scatsid_relationship"

    def __init__(self):
        self.operate_situation = {}
        self.operator_result = {}
        self.subsys_relation = {}

        # self.subsys_intid_match()
        self.int_statistic_record = {}

    def get_operate_data(self):
        db = Postgres.get_instance()
        result = db.call_pg_data(SituationOperate.sql)
        return result

    def operate_statistic(self):
        db = Postgres.get_instance()
        if IFTEST:
            result = db.call_pg_data(SituationOperate.sql_operate_statistic_test)
        else:
            result = db.call_pg_data(SituationOperate.sql_operate_statistic)
        if len(result)>0:
            result['oper_type'] = result['all_type'].apply(lambda x: list(set(x.split(','))))
            # self.operator_statistic(result)
            self.int_statistic(result)
            return result
        else:
            print("can't get operate data!please check database table and time!")
            return None

    def operator_statistic(self,oper_data):
        grouped = oper_data.groupby(['userid', 'operdate'])
        for (k1, k2), group in grouped:
            operator = self.Operator()
            operator_record = operator.oper_record
            all_type = operator_record.get('OperRecord').keys()
            userid = k1
            operdate = k2
            operator_record['UserId'] = userid
            operator_record['OperDate'] = operdate
            for type_list in group['oper_type'].values:
                for type in type_list:
                    operator_record['OperNum'] += 1
                    if type in all_type:
                        operator_record['OperRecord'][type] += 1
                    else:
                        operator_record['OperRecord'][type] = 0

            # print(operator.oper_record)
            self.operator_result.append(operator)

    def int_statistic(self,oper_data):

        grouped = oper_data[oper_data['scats_id'] != None].groupby(['scats_id', 'operdate'])
        grouped2 = oper_data[oper_data['subsystem_id'] != None].groupby(['subsystem_id', 'operdate'])

        for (k1, k2), group in grouped:
            int = self.IntOperate()
            int_record = int.oper_record
            all_type = int_record.get('OperRecord').keys()
            int_id = k1
            operdate = k2
            int_record['IntId'] = int_id
            int_record['OperDate'] = operdate
            for type_list in group['oper_type'].values:
                int_record['OperNum'] += 1
                for type in type_list:
                    if type in all_type:
                        int_record['OperRecord'][type] += 1
                    else:
                        int_record['OperRecord'][type] = 1
            self.int_statistic_record[int_id] = int.oper_record
        recorded_int = self.int_statistic_record.keys()

        for (k1, k2), group in grouped2:
            subsys_id = k1
            int_list = self.subsys_relation.get(k1)
            if int_list:
                for int in int_list:
                   if int in recorded_int:
                       for type_list in group['oper_type'].values:
                           self.int_statistic_record.get(int)['OperNum'] += 1
                           for type in type_list:
                               self.int_statistic_record.get(int)['OperRecord'][type] += 1
                   else:
                       int2 = self.IntOperate()
                       int_record = int2.oper_record
                       all_type = int_record.get('OperRecord').keys()
                       int_id = int
                       operdate = k2
                       int_record['IntId'] = int_id
                       int_record['OperDate'] = operdate
                       for type_list in group['oper_type'].values:
                           int_record['OperNum'] += 1
                           for type in type_list:
                               if type in all_type:
                                   int_record['OperRecord'][type] += 1
                               else:
                                   int_record['OperRecord'][type] = 1

                       # print(int2.oper_record)
                       self.int_statistic_record[int_id] = int2.oper_record
        # print(self.int_statistic_record)

    def subsys_intid_match(self):
        db = Postgres.get_instance()
        result = db.call_pg_data(SituationOperate.sql_subsys_inf)
        grouped = result.groupby(['subsystem_id'])
        for k1, group in grouped:
            self.subsys_relation[k1] = group['site_id'].tolist()
        # print(self.subsys_relation)
        return result

    def data_solve(self):
        all_int = self.int_statistic_record.keys()
        df_value = []
        for key in all_int:
            value = self.int_statistic_record.get(key)
            df_value.append(value)

        df = pd.DataFrame(df_value)
        if len(df)>0:
            df['OperNum'] = df['OperNum'].apply(lambda x: x/10 if x/10 <= 1 else 1)
            # print(df)
        else:
            pass
        return df

    def call_pg_function(self,function_name,args):
        db = Postgres.get_instance()
        result = None
        if db.conn is not None:
            try:
                result = db.cr.callproc(function_name, args)
                db.conn.commit()
                db.db_close()
            except Exception as e:
                print('call_pg_function error function_name is %s'% function_name, e)
        else:
            conn, cr = db.db_conn()
            if conn:
                try:
                    assert cr is not None
                    result = db.cr.callproc(function_name, args)
                except psycopg2.IntegrityError as e:
                    print(e)
                finally:
                    conn.commit()
                    db.db_close()
            else:
                logger2.error("数据库连接失败！")
            # except Exception as e:
            #     print('call_pg_function error function_name is %s'%function_name, e)
        return result

    class Operator():

        def __init__(self):
            self.oper_record = {'UserId':None, 'OperDate':'','OperNum':0 ,'OperRecord':{'Cycle':0,'Split':0,'Dwell':0,'XSF':0,'Coordination':0}}

    class IntOperate():

        def __init__(self):
            self.oper_record = {'IntId': None, 'OperDate': '', 'OperNum': 0,
                                'OperRecord': {'Cycle': 0, 'Split': 0, 'Dwell': 0, 'XSF': 0, 'Coordination': 0}}


def so_run():
    db = Postgres.get_instance()
    interval = SO_INTERVAL
    local_time = dt.datetime.now()
    stime = local_time-dt.timedelta(minutes=15)
    etime = local_time
    stime = dt.datetime.strftime(stime, '%Y-%m-%d %H:%M:%S')
    etime = dt.datetime.strftime(etime, '%Y-%m-%d %H:%M:%S')
    # stime = '2018-10-22 00:00:00'
    # etime = '2018-10-22 23:00:00'
    args = [stime, etime, interval]
    S1 = SituationOperate()
    # print('123')
    try:
        S1.call_pg_function(S1.sql_alarm_operate_match, args)
    except Exception as e:
        logger2.error('so_run: %s'% e)
    else:
        logger2.info("完成一轮调控记录匹配")
    finally:
        db.db_close()





if __name__ == '__main__':
    # demo_pg_inf = {'database': "signal_specialist",'user': "postgres",'password': "postgres",
    #           'host': "192.168.20.46",'port': "5432"}
    # stime = '2018-10-22 00:00:00'
    # etime = '2018-10-22 12:00:00'
    # interval = '15minutes'
    # args = [stime,etime,interval]
    # S1 = SituationOperate()
    # result = S1.call_pg_function(S1.sql_alarm_operate_match,args)
    # print(result)
    so_run()

