import psycopg2
import datetime as dt
from proj.config import database
from proj.python_project.scats_operate_parsing import seperate_operate_record
from proj.python_project.scats_operate_parsing import database_gai
from proj.python_project.scats_operate_parsing import content


# 所有路口类
class NetState():
    def __init__(self, int_list, net_init_state=None):
        self.net_state = {}
        self.init(int_list, net_init_state)
        pass

    def init(self, int_list, net_init_state):
        # scats_split_init[scats_id] = {'updatatime': None, 'user_name': None, 'spilt': None, 'cycle': None,
        #                               'coordination': None,
        #                               'pp': None, 'xsf': None, 'other': None, 'dwell': None, 'siteid': scats_id}
        if net_init_state:
            for key, value in net_init_state.items():
                self.net_state[str(key)] = {'UpdateTime': value.get('updatatime'), 'User': value.get('user_name'),
                                            'Spilt': value.get('spilt'), 'Cycle': value.get('cycle'), 'Coordination':
                                                value.get('updatatime'), 'PP': value.get('pp'),
                                            'Dwell': value.get('dwell'), 'XSF': value.get('xsf'),
                                            'Other': value.get('other'), 'Siteid': value.get('siteid')}
        else:
            for i in int_list:
                self.net_state[str(i)] = None
        # print(self.net_state)

    # 更新所有路口状态
    def int_status_update(self, operate_dada):

        for data in operate_dada:
            message = data[3]
            user_id = data[0]
            scats_id = data[5]
            user = data[6]
            last_state = self.net_state.get(scats_id)
            int_state = self.State(scats_id)
            int_state.User = user
            int_state = self.state_feedback(int_state, message)
            if last_state:
                int_state.update_state(last_state)
            else:
                self.net_state[scats_id] = int_state.state

    def state_feedback(self, int_state, message):
        # state = None
        if message != '':
            message = eval(message)
            for key in message.keys():
                if key != 'extra':
                    if len(message[key]) == 1 and '/' in str(message[key]):
                        state = 'unlock'
                    else:
                        state = 'lock'
                    if key == 'PL' or key == 'IP' or key == 'SP' or key == '0PD' or key == 'Plan':
                        if '#' in str(message[key]):
                            if 'KEY' in str(message['extra']):
                                int_state.Spilt = 'other'
                            else:
                                int_state.Spilt = 'fixed'
                        else:
                            int_state.Spilt = 'adaptive'
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
                           'User': int_state.User, 'Spilt': int_state.Spilt, 'Cycle': int_state.Cycle,
                           'Coordination': int_state.Coordination
            , 'PP': int_state.PP, 'Dwell': int_state.Dwell, 'XSF': int_state.XSF, 'Other': int_state.Other,
                           'Siteid': int_state.Siteid}
        return int_state

    # 单个路口信息类
    class State():
        '''初始化属性'''

        def __init__(self, Siteid):
            self.Siteid = Siteid
            self.state = {}
            self.User = None
            self.Spilt = None
            self.Cycle = None
            self.Coordination = None
            self.PP = None
            self.Dwell = None
            self.XSF = None
            self.Other = None
            self.state = {'UpdateTime': str(dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')),
                          'User': self.User, 'Spilt': self.Spilt, 'Cycle': self.Cycle, 'Coordination': self.Coordination
                , 'PP': self.PP, 'Dwell': self.Dwell, 'XSF': self.XSF, 'Other': self.Other}

        def update_state(self, last_state):
            for key, value in self.state.items():
                if value:
                    last_state[key] = value
            return last_state

        # 状态反馈


# # oracle链接数据库获取数据
# def get_record_oracle():
#     OracleUser = 'enjoyor/admin@33.83.100.139/orcl'
#     conn = cx_Oracle.connect(OracleUser)
#     cur = conn.cursor()
#     sql = "select SITEID from INTERSECT_INFORMATION"
#     cur.execute(sql)
#     x = cur.fetchall()
#     cur.close()
#     conn.close()
#     return x


# pg链接数据库获取数据
# def get_record_pg():
#     pg_inf1 = {'database': "zkr", 'user': "postgres", 'password': "postgres",
#                'host': "192.168.20.46", 'port': "5432"}
#     conn = psycopg2.connect(database=pg_inf1['database'], user=pg_inf1['user'], password=pg_inf1['password'],
#                             host=pg_inf1['host'], port=pg_inf1['port'])
#     cur = conn.cursor()
#     sql = "select opertime,opertype,siteid,userid,message from record_data_parsing"
#     cur.execute(sql)
#     x = cur.fetchall()
#     cur.close()
#     conn.close()
#     return x


# 获取路口id
# def get_siteid(data):
#     list = []
#     for i in data:
#         siteid = i[2]  # i=111
#         if siteid != None:
#             if 'I=' in siteid:
#                 siteid = siteid[2:]
#                 list.append(siteid)
#             else:
#                 list.append('')
#         else:
#             list.append('')
#     return list


# def insert(data):
#     print(data)
#     pp = demo_pg_inf
#     db = psycopg2.connect(dbname=pp['database'], user=pp['user'], password=pp['password'], host=pp['host'],
#                           port=pp['port'])
#     cr = db.cursor()
#     try:
#         for value in data.values():
#             list=[]
#             updatatime=value['UpdateTime']
#             user=value['User']
#             spilt = value['Spilt']
#             cycle = value['Cycle']
#             coordination = value['Coordination']
#             pp = value['PP']
#             dwell = value['Dwell']
#             xsf = value['XSF']
#             other = value['Other']
#             siteid = value['Siteid']
#             list=[updatatime,user,spilt,cycle,coordination,pp,dwell,xsf,other,siteid]
#             #sql2="delete from scats_int_state_feedback where siteid=siteid"
#             sql1 = "insert into scats_int_state_feedback_copy(updatatime,user_name,spilt,cycle,coordination,pp,dwell,xsf,other,siteid) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#             #cr.execute(sql2)
#             cr.execute(sql1, list)
#             db.commit()
#     except psycopg2.IntegrityError:
#         pass
#     cr.close()
#     db.close()

def insert(operate_int_states):
    pg = database.Postgres.get_instance()
    sql_del = content.CONTENT.sql9
    conn, cr = pg.db_conn()
    try:
        cr.execute(sql_del)
        conn.commit()
    except Exception as e:
        print('insert', e)
        conn.commit()

    all_result = []

    for key in operate_int_states.keys():
        siteid = key
        value = operate_int_states.get(key)  # 读取一次操作记录后更新值
        if value:
            updatetime = value['UpdateTime']
            user = value['User']
            spilt = value['Spilt']
            cycle = value['Cycle']
            coordination = value['Coordination']
            pp = value['PP']
            dwell = value['Dwell']
            xsf = value['XSF']
            other = value['Other']
            list = [updatetime, user, spilt, cycle, coordination, pp, dwell, xsf, other, siteid]
            all_result.append(list)
    # print(all_result)
    pg = database.Postgres.get_instance()
    pg.send_pg_data(content.CONTENT.sql10, data=all_result)
    return


if __name__ == '__main__':
    pg = database_gai.Postgres()
    result = pg.call_pg_data("select * from record_data_parsing where opertime between '2018-10-30' and '2018-10-31'")
    int_list = ['1', '2', '3', '4']
    net_state = NetState(int_list)
    net_state.int_status_update(result)
    # print(net_state.net_state)
    insert(net_state.net_state)
