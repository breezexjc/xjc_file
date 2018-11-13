import os
import sys
import time
import psycopg2
import database
import cx_Oracle
import datetime as dt
from proj.proj.config.database import Postgres,Oracle


#所有路口类
class NetState():
    def __init__(self,int_list):
        self.net_state = {}
        self.init(int_list)
        pass
    def init(self,int_list):
        for i in int_list:
            self.net_state[str(i)] = None
        print(self.net_state)

    # 更新所有路口状态
    def int_status_update(self, operate_dada):
        for data in operate_dada:
            message = data[3]
            scats_id = data[5]
            int_state = self.State(scats_id)
            int_state = self.state_feedback(int_state, message)
            try:
                last_state = self.net_state[scats_id]
            except KeyError:
                self.net_state[scats_id] = None
                last_state = self.net_state[scats_id]
            print(last_state)
            if last_state:
                new_state = int_state.update_state(last_state)
                self.net_state[scats_id] = new_state
            else:
                self.net_state[scats_id] = int_state.state


    def state_feedback(self, int_state,message):
        state = None
        if message != '':
            message = eval(message)
            for key in message.keys():
                if key != 'extra':
                    if len(message[key]) == 1 and '/' in message[key]:
                        state = 'unlock'
                    else:
                        state = 'lock'
                    if key == 'PL' or key == 'IP' or key == 'SP' or key == '0PD' or key == 'Plan':
                        int_state.Spilt = state
                    elif key == 'CL' or key == 'HCL' or key == 'SCL' or key == 'LCL' or key == 'XCL':
                        int_state.Cycle = state
                    elif key == 'LP' or key == 'LP0' or key == 'MG' or key == 'DV':
                        int_state.Coordination = state
                    elif key == 'PP':
                        int_state.PP = state
                    elif key == 'Dwell':
                        int_state.Dwell = state
                    elif key == 'XSF':
                        int_state.XSF = state
                    elif key == 'VF' or key == 'SD' or key == 'D#':
                        int_state.Other = state
                    else:
                        pass
                else:
                    pass
        else:
            pass
        int_state.state = {'UpdateTime': str(dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')),
                  'User': int_state.User, 'Spilt': int_state.Spilt, 'Cycle': int_state.Cycle, 'Coordination': int_state.Coordination
        , 'PP': int_state.PP, 'Dwell': int_state.Dwell, 'XSF': int_state.XSF, 'Other': int_state.Other}
        return int_state

    # 单个路口信息类
    class State():
        '''初始化属性'''
        def __init__(self, Siteid):
            self.Siteid = Siteid
            self.state ={}
            self.User = None
            self.Spilt = None
            self.Cycle = None
            self.Coordination = None
            self.PP = None
            self.Dwell = None
            self.XSF = None
            self.Other = None
            self.state = {'UpdateTime': str(dt.datetime.strftime(dt.datetime.now(),'%Y-%m-%d %H:%M:%S')), 'User':self.User,'Spilt':self.Spilt,'Cycle':self.Cycle,'Coordination':self.Coordination
                          ,'PP':self.PP,'Dwell':self.Dwell,'XSF':self.XSF,'Other':self.Other}

        def update_state(self, last_state):
            for key,value in self.state.items():
                if value:
                    last_state[key] = value
            return last_state


        # 状态反馈





#oracle链接数据库获取数据
def get_record_oracle():
    OracleUser = 'SIG_OPT_ADMIN/admin@192.168.20.56/orcl'
    conn = cx_Oracle.connect(OracleUser)
    cur=conn.cursor()
    sql="select SITEID from INTERSECT_INFORMATION"
    cur.execute(sql)
    x=cur.fetchall()
    cur.close()
    conn.close()
    return x

#pg链接数据库获取数据
def get_record_pg():
    pg_inf1 = {'database': "signal_specialist", 'user': "postgres", 'password': "postgres",
               'host': "192.168.20.46", 'port': "5432"}
    conn = psycopg2.connect(database=pg_inf1['database'], user=pg_inf1['user'], password=pg_inf1['password'],
                            host=pg_inf1['host'], port=pg_inf1['port'])
    cur = conn.cursor()
    sql = "select opertime,opertype,siteid,userid,message from record_data_parsing"
    cur.execute(sql)
    x = cur.fetchall()
    cur.close()
    conn.close()
    return x

#获取路口id
def get_siteid(data):
    list=[]
    for i in data:
        siteid=i[2]         #i=111
        if siteid!=None:
            if 'I=' in siteid:
                siteid=siteid[2:]
                list.append(siteid)
            else:
                list.append('')
        else:
            list.append('')
    return list




if __name__=='__main__':
    # record=get_record()
    # site=get_siteid(record)
    # for i in range(len(site)):
    #     print('11')
    #     state=State(record[i][2])
    #     user_id=state.get_userid(record[i])
    #     user_name=state.get_user(user_id)
    #     state_modify=state.state_feedback(record[i][4])
    #     state.insert()
    # print('状态输出完成')
    # site_all=get_record_oracle()
    # list_site_all=[]
    # for i in range(len(site_all)):
    #     list_site_all.append(site_all[i][0])
    # N = NetState(list_site_all)

    sql = """
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
				AND opertime BETWEEN '2018-10-29 00:00:00'
				AND '2018-10-29 12:00:00'
				ORDER BY
					userid,
					siteid,
					opertime
			) A
	) A
LEFT JOIN subid_scatsid_relationship C ON C .subsystem_id = A .subsystem_id
LEFT JOIN sms_user d ON A .userid = d.company_id
    """
    pg = Postgres()
    result = pg.call_pg_data(sql)
    print(result)
    int_list = ['1','2','3','4']
    net_state = NetState(int_list)
    net_state.int_status_update(result)
    print(net_state.net_state)


