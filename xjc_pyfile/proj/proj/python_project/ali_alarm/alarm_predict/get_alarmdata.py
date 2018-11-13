#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import pandas as pd
# class PGCONN(object):
#     ip = '192.168.20.45'
#     database = 'research'
#     port = 5432
#     account = 'postgres'
#     password = 'postgres'
#
#     def __init__(self):
#         self.ip = PGCONN.ip
#         self.database = PGCONN.database
#         self.port = PGCONN.port
#         self.account = PGCONN.account
#         self.password = PGCONN.password
#     def conn(self):
#         try:
#             self.conn = psycopg2.connect(database=self.database,user=self.account,password=self.password,
#                                     host=self.ip,
#                                     port=self.port)
#         except Exception as e:
#             print(self.ip +" connect failed")
#         else:
#             print(self.ip +" connect succeed")
#             self.cr = self.conn.cursor()
#             return self.conn, self.cr
#
#     def close(self):
#         self.cr.close()
#         self.conn.close()


class DbConn:
    ip = '192.168.20.45'
    database = 'research'
    port = 5432
    account = 'postgres'
    password = 'postgres'

    def __init__(self):
        self.ip = DbConn.ip
        self.database = DbConn.database
        self.port = DbConn.port
        self.account = DbConn.account
        self.password = DbConn.password
        self.conn = []
        self.cr = []

    def db_conn(self):
        try:
            self.conn = psycopg2.connect(database=self.database,user=self.account,password=self.password,
                                    host=self.ip,
                                    port=self.port)
        except Exception as e:
            print(self.ip +" connect failed")
            return self.conn, self.cr
        else:
            print(self.ip +" connect succeed")
            self.cr = self.conn.cursor()
            return self.conn, self.cr

    def close(self):
        if self.conn:
            self.cr.close()
            self.conn.close()


def tuple2frame(result, index):
    column_name = []
    for i in range(len(index)):
        index_name = index[i][0]
        column_name.append(index_name)
    print(column_name)
    result = pd.DataFrame(result, columns=column_name)
    return result


def get_alarmdata(isframe,sql):
    db = DbConn()
    conn, cr = db.db_conn()
    # sql = "select scats_id,inter_name,date_day,time_point,vehicle_dir from alarm_temp where date_day " \
    #       " ='{0}'  and time_point between '{1}' and '{2}'order by date_day,time_point,scats_id"\
    #     .format(sdate, stime, etime)
    cr.execute(sql)
    index = cr.description
    result = cr.fetchall()
    db.close()
    # print(index)
    if result:
        if isframe:
            fresult = tuple2frame(result, index)
            return fresult
        else:
            return result
    else:
        return []


def create_alarm_serice(time_serice, alarm_data, conn_alarm_data):
    # print(conn_alarm_data)
    # input()

    scats_id = alarm_data['scats_id'].drop_duplicates().values
    # veh_dir = alarm_data['vehicle_dir'].drop_duplicates().values
    # print(scats_id,veh_dir)
    serice_result = []
    scats_serice = {}
    # for i in scats_id:


    # match_conn_alarm_data = conn_alarm_data[conn_alarm_data['up_node'] == i]
    # veh_dir = match_alarm_data['vehicle_dir'].drop_duplicates().values
    up_node = conn_alarm_data['up_node'].drop_duplicates().values.tolist()
    down_node = conn_alarm_data['down_node'].drop_duplicates().values.tolist()
    veh_dir_down = conn_alarm_data['import_desc'].drop_duplicates().values.tolist()
    veh_dir_up = conn_alarm_data['export_desc'].drop_duplicates().values.tolist()
    print(veh_dir_down)
    # 上下游路段报警？
    # match_conn_alarm_data = conn_alarm_data[((conn_alarm_data['up_node'] == i)& (conn_alarm_data['export_desc'] == j))
    #                                         | ((conn_alarm_data['down_node'] == i)& (conn_alarm_data['import_desc'] == j))]
    # match_conn_alarm_data = conn_alarm_data[((conn_alarm_data['up_node'] == up_node)& (conn_alarm_data['export_desc'] == j))
    #                                         | ((conn_alarm_data['down_node'] == down_node)& (conn_alarm_data['import_desc'] == j))]
    match_conn_alarm_data = conn_alarm_data
    match_alarm_data = alarm_data[((alarm_data['scats_id'] == up_node[0]) & (alarm_data['vehicle_dir'] == veh_dir_up[0]))
                                  | ((alarm_data['scats_id'] == down_node[0]) & (alarm_data['vehicle_dir'] == veh_dir_down[0]))]
    if len(match_conn_alarm_data) > 0:
        print(match_conn_alarm_data)
        time_interval = [i[2].split('~') for i in match_conn_alarm_data.values]
        print(time_interval)
        for m in time_serice:
            for n in time_interval:
                pass
        # alarm_conn_alarm_data =
    # match_data2 = match_alarm_data[alarm_data['vehicle_dir'] == j]
    high_delay_data = match_alarm_data[match_alarm_data['delay_value'] >= 60]
    print(high_delay_data)
    normal_delay_data = match_alarm_data[match_alarm_data['delay_value'] < 60]
    alarm_time_serice_high = high_delay_data['time_point'].values
    alarm_time_serice_normal = normal_delay_data['time_point'].values
    alarm_time_serice_high2str = [str(i) for i in alarm_time_serice_high]
    alarm_time_serice_normal2str = [str(i) for i in alarm_time_serice_normal]
    # print(alarm_time_serice_high2str)
    status_serice = []
    for m in time_serice:
        time = str(m)[11:]
        if time in alarm_time_serice_high2str:
            status = 2
        elif time in alarm_time_serice_normal2str:
            status = 1
        else:
            status = 0
        status_serice.append(status)


    vehicle_serice = {"StatusSerice": status_serice}
    # serice_result.append(vehicle_serice)
    scats_serice["ScatsSerice"]= serice_result
    # print(scats_serice)
    # print(scats_id)
    return vehicle_serice


def create_condition(bayes):
    key_list = bayes.keys()
    result = {}
    int_id_list = []
    rdsectid_all = []
    for key in key_list:
        up_match = bayes[key][0]
        down_match = bayes[key][1]
        up_int = []
        down_int = []
        for i in up_match:
            # ['UTRSS001056', '526', '1', '北进口', '南出口', '莫干山路（密度桥路-天目山路）']
            rdsectid = i[0]
            int_id = i[2]
            rdsect_dirdesc = i[3]
            up_int.append([rdsectid, int_id, rdsect_dirdesc])
            rdsectid_all.append(rdsectid)
            if int_id not in int_id_list:
                int_id_list.append(int_id)

        for i in down_match:
            # ['UTRSS001056', '526', '1', '北进口', '南出口', '莫干山路（密度桥路-天目山路）']
            rdsectid = i[0]
            int_id = i[2]
            rdsect_dirdesc = i[3]
            down_int.append([rdsectid, int_id, rdsect_dirdesc])
            rdsectid_all.append(rdsectid)
            if int_id not in int_id_list:
                int_id_list.append(int_id)
        result[key] = [up_int + down_int]
    return result, int_id_list, rdsectid_all


def count_statusserices(status):
    data = status.get('StatusSerice')
    for i in data:
        pass

 
def main(sdate,stime,etime,rdsectid = None, self_rdsect = False):
    isframe = 1
    match_data = bayes_net(rdsectid)
    # select_net = bayes[rdsectid]
    # print(select_net)
    # condition, int_select, rdsectid_select = create_condition(bayes)
    # match_data = create_condition(bayes)
    up_node = match_data.iloc[0, 1]
    down_node = match_data.iloc[0, 2]
    rdsectid_select = []
    int_select = [up_node, down_node]

    if rdsectid is None:
        sql = "select scats_id,inter_name,date_day,time_point,vehicle_dir,delay_value from alarm_temp where date_day " \
              " ='{0}'  and time_point between '{1}' and '{2}'order by date_day,time_point,scats_id" \
            .format(sdate, stime, etime)
        sql2 = "select rdsectid,up_to_down_match_num,time_interval,length,score from alarm_rdsect_match_result where date_day = '{0}' and left(time_interval,7)" \
              " between '{1}' and '{2}' ".format(sdate, stime, etime)
    else:
        int_id = ''
        for i in int_select:
            int_id = int_id+ '\''+ i +'\','

        sql = "select scats_id,inter_name,date_day,time_point,vehicle_dir,delay_value from alarm_temp where date_day " \
              " ='{0}'  and time_point between '{1}' and '{2}' and scats_id in ({3})order by date_day,time_point,scats_id" \
            .format(sdate, stime, etime, int_id[:-1])

        rdsect_id = '\'' + rdsectid + '\','
        for i in rdsectid_select:
            rdsect_id = rdsect_id + '\'' + i + '\','

        # sql2 = "select rdsectid,up_to_down_match_num,time_interval,length,score from alarm_rdsect_match_result " \
        #        "where date_day = '{0}' and left(time_interval,7)" \
        #        " between '{1}' and '{2}' and rdsectid in ({3})".format(sdate, stime, etime, rdsect_id[:-1])

        sql2 = "SELECT a.rdsectid, a.up_to_down_match_num,a.time_interval,a.LENGTH,a.score,b.up_node,b.down_node, b.import_desc, b.export_desc" \
               " FROM alarm_rdsect_match_result a, alarm_rdsect b WHERE a.rdsectid = b.rdsectid and  " \
               "a.date_day = '{0}' AND LEFT (a.time_interval, 8) BETWEEN '{1}' AND '{2}' AND " \
               "a.rdsectid IN ({3})".format(sdate, stime, etime, rdsect_id[:-1])

        print(sql2)
        # input()

    alarm_data = get_alarmdata(isframe, sql)
    # match_data = alarm_data[alarm_data['scats_id'] in int_select]
    if len(alarm_data) > 0:
        # print(alarm_data)
        start_time = sdate + ' ' + stime
        end_time = sdate + ' ' + etime
        time_serice = pd.date_range(start=start_time, end=end_time, freq='2min')
        # help(pd.date_range)
        print(time_serice)
        # 获取关联报警数据
        conn_alarm_data = get_alarmdata(isframe, sql2)
        print("============================conn_alarm_data================================")
        print(conn_alarm_data)
        # 创建道路状态时间序列
        if len(conn_alarm_data)>0:
            alarm_serice = create_alarm_serice(time_serice, alarm_data, conn_alarm_data)
            print("=============================alarm_serice=================================")
            print(alarm_serice)


    else:
        print("无报警数据")



def bayes_net(select_rdsectid = None):
    isframe = 1
    sql3 = "select rdsectid,up_node,down_node,import_desc,export_desc,fstr_desc from alarm_rdsect "
    rdsect_data = get_alarmdata(isframe, sql3)
    print(rdsect_data)
    if select_rdsectid is None:
        bayesnet_all = {}
        # rdsectid_list = rdsect_data['rdsectid'].drop_duplicates().values
        for i in rdsect_data.values:
            rdsectid = i[0]
            up_node = i[1]
            down_node = i [2]
            opposite = rdsect_data['rdsectid'][(rdsect_data['down_node'] == up_node) & (rdsect_data['up_node'] == down_node)]
            opp_rdsectid = opposite.values.tolist()[0]
            up_match = rdsect_data['rdsectid'][(rdsect_data['down_node'] == up_node) & (rdsect_data['rdsectid'] != opp_rdsectid)]
            down_match = rdsect_data['rdsectid'][(rdsect_data['up_node'] == down_node) & (rdsect_data['rdsectid'] != opp_rdsectid)]
            bayesnet = [up_match.values.tolist(), down_match.values.tolist()]
            print("bayesnet:",bayesnet)
            bayesnet_all[rdsectid] = bayesnet
        return bayesnet_all
    else:
        bayesnet_all = {}
        # rdsectid_list = rdsect_data['rdsectid'].drop_duplicates().values
        match_data = rdsect_data[rdsect_data['rdsectid'] == select_rdsectid]
        # print(match_data)
        rdsectid = select_rdsectid
        up_node = match_data.iloc[0, 1]
        down_node = match_data.iloc[0, 2]
        opposite = rdsect_data['rdsectid'][
            (rdsect_data['down_node'] == up_node) & (rdsect_data['up_node'] == down_node)]
        opp_rdsectid = opposite.values.tolist()[0]
        up_match = rdsect_data[
            (rdsect_data['down_node'] == up_node) & (rdsect_data['rdsectid'] != opp_rdsectid)]
        down_match = rdsect_data[
            (rdsect_data['up_node'] == down_node) & (rdsect_data['rdsectid'] != opp_rdsectid)]
        # bayesnet = [up_match.values.tolist(), down_match.values.tolist()]
        bayesnet = match_data
        print("bayesnet:", bayesnet)
        bayesnet_all[rdsectid] = bayesnet
        return bayesnet

# # 获取关联报警状态
# def get_conn_alarm(sdate, stime, etime, isframe, sql):
#     fresult = []
#     sql = "select * from alarm_rdsect_match_result where date_day = '{0}' and left(time_interval,7)" \
#           " between '{1}' and '{2}' ".format(sdate, stime, etime)
#     db = DbConn()
#     conn, cr = db.db_conn()
#     cr.execute(sql)
#     index = cr.description
#     result = cr.fetchall()
#     db.close()
#     if result and isframe:
#         fresult = tuple2frame(result, index)
#         return fresult
#     else:
#         return result
#





if __name__ == '__main__':
    sdate = '2018-07-02'
    stime = '07:00:00'
    etime = '09:00:00'
    self_rdsect = True
    main(sdate, stime, etime, 'UTRSS001028', self_rdsect)
    # bayes_net()