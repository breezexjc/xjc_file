#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2 as pg
import pandas as pd
from ali_alarm import GlobalContent
import numpy as np
from sklearn.cluster import KMeans
from sklearn.externals import joblib
from sklearn import cluster


import matplotlib.pyplot as plt
import matplotlib.dates as mdate
import datetime as dt
plt.style.use('seaborn-whitegrid') #使用样式
plt.rcParams['font.sans-serif'] = ['SimHei'] #显示中文
plt.rcParams['axes.unicode_minus'] = False #显示负号




class GROUP:
    def __init__(self):
        self.group_id = None
        self.rdsectids = []
        self.time_interval = 'demo'


# 根据日期选择报警数据
def call_alarm_data(date_day, end_day):
    try:
        conn = pg.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
    except Exception as e:
        print(e)
    else:
        cr_pg = conn.cursor()
        sql2 = "select a.scats_id,a.inter_name,a.date_day,a.time_point,a.vehicle_dir,a.delay_value, b.rdsectid,b.fstr_desc,b.length " \
               "from (select * from alarm_record where date_day >= '{0}' and date_day <= '{1}')a,alarm_rdsect b " \
               "where (a.scats_id = b.down_node and b.import_desc = a.vehicle_dir) or (a.scats_id = b.up_node " \
               "and b.export_desc = a.vehicle_dir) ORDER BY b.rdsectid,a.date_day ,a.time_point".format(date_day, end_day)
        cr_pg.execute(sql2)
        result = cr_pg.fetchall()
        frame = pd.DataFrame(result)
        frame.columns = ['scats_id', 'int_name', 'date_day', 'time_point', 'vehicle_dir', 'delay_value', 'rdsectid',
                         'rdsect_desc', 'length']
        conn.close()
        # tru2list = [list(i) for i in result]
        return frame


# 列表去除最后一个为null的元素
def list_del(list):
    list2 = list
    for i in range(len(list)):
        L = len(list)
        if list[L-i-1]== 10:
            del list2[L-i-1]
            break
    return list2


def draw_alarm(alarm_data):
    rdsect_list = (alarm_data['rdsectid'].drop_duplicates())
    # print(rdsect_list)
    for i in range(len(rdsect_list)):
        rdsectid = rdsect_list.iloc[i]
        match_data = alarm_data[alarm_data['rdsectid'] == rdsectid]
        alarm_time = match_data['time_point']
        veh_dir = match_data['vehicle_dir']
        datetime_start = dt.datetime.strptime("07:00:00", "%H:%M:%S")
        datatime_1day = dt.timedelta(hours=13)
        datetime_end = datetime_start + datatime_1day
        start_time = pd.date_range(datetime_start, datetime_end, freq='120s')
        alarm_time2 = np.array(alarm_time).tolist()
        alarm_time2 = [dt.datetime.strptime(str(i), '%H:%M:%S') for i in alarm_time2]
        delay_value = match_data['delay_value']
        rdsect_neme = match_data.iloc[0][7]
        list_entrance = []
        list_exit = []

        # print(start_time[j].time())
        # print(dt.datetime.strptime(str(alarm_time.iloc[j]), "%H:%M:%S"))
        # if start_time[j].time() in alarm_time2:
        #     match_data2 = match_data[match_data['time_point'] == start_time[j].time()]
        #     print(match_data2['vehicle_dir'].values)
        for j in range(len(veh_dir)):
            # print((veh_dir.iloc[j])[1:])

            if (veh_dir.iloc[j])[1:] == '进口':
                a = delay_value.iloc[j]
                b = 10
            else:
                a = 10
                b = delay_value.iloc[j]

            # if (match_data2['vehicle_dir'].values)[1:] == '进口':
            #     a = match_data2['delay_value'].values
            #     # a = delay_value.iloc[j]
            #     b = 10
            #     print(1)
            #     # time_entrace.append(alarm_time.iloc[j])
            # else:
            #     a = 10
            #     b = match_data2['delay_value'].values
            #     print(b)
            #     # b = delay_value.iloc[j]
            #     # time_exit.append(alarm_time.iloc[j])
            list_entrance.append(a)
            list_exit.append(b)

        fig = plt.figure(figsize=(16, 10))
        ax1 = fig.add_subplot(111)
        # ax2 = fig.add_subplot(212)
        # start_time = np.array(alarm_time).tolist()
        # start_time = [dt.datetime.strptime(str(i), '%H:%M:%S') for i in start_time]
        # print(list_entrance)
        list_entrance2 = []
        list_exit2 = []
        for j in range(len(start_time)):
            o = 0
            for m in range(len(alarm_time2)):
                if start_time[j] == alarm_time2[m]:
                    o += 1
                    list_entrance2.append(list_entrance[m])
                    list_exit2.append(list_exit[m])
            if o == 0:
                list_entrance2.append(10)
                list_exit2.append(10)
            if o == 2:
                list_entrance2 =list_del(list_entrance2)
                list_exit2 = list_del(list_exit2)
        ax1.plot(start_time, list_entrance2, lw=2, color='blue', label="下游报警", marker='^')
        ax1.plot(start_time, list_exit2, lw=2, color='red', label="上游报警", marker='*')
        ax1.xaxis.set_major_formatter(mdate.DateFormatter('%H'))
        datetime_start = dt.datetime.strptime("07:00:00", "%H:%M:%S")
        datatime_1day = dt.timedelta(hours=13)
        datetime_end = datetime_start + datatime_1day
        ax1.set_xlim([datetime_start, datetime_end])
        ax1.set_ylim([0, 100])
        ax1.set_xticks(pd.date_range(datetime_start, datetime_end, freq='1H'))
        ax1.legend(loc='upper right')
        ax1.set_title(rdsect_neme+"路段上下游报警规律",fontsize = 20)
        ax1.set_xlabel('时间（h）',fontsize = 20)
        ax1.set_ylabel('报警',fontsize = 20)
        ax1.grid(True)
        fig.savefig("G:\程序文件\my\报警规律3\\"+rdsect_neme + '.png', dpi=75)
        plt.close()
        # plt.show()
        print(rdsect_neme)
        # print(match_data)


def draw_alarm_connect(rdsect_dict):
    from mpl_toolkits.mplot3d import Axes3D
    downstream = []
    connect_rate = []
    length = []
    for i in range(len(rdsect_dict)):
        # downstream.append(rdsect_dict[i][1])
        # length.append(rdsect_dict[i][4])
        if rdsect_dict[i][1] > 0 and rdsect_dict[i][3] / rdsect_dict[i][1]>0:
            downstream.append(rdsect_dict[i][1])
            length.append(rdsect_dict[i][4])
            # if rdsect_dict[i][3] / rdsect_dict[i][1] > 1:
            #     print(rdsect_dict[i])
            connect_rate.append(round(rdsect_dict[i][3]/rdsect_dict[i][1], 2))
        else:
            pass
            # connect_rate.append(0)
    fig = plt.figure(figsize=(8, 5))
    ax1 = fig.add_subplot(211)
    ax2 = fig.add_subplot(212)
    downstream = np.array(downstream).tolist()
    connect_rate = np.array(connect_rate).tolist()
    length = np.array(length).tolist()
    ax1.scatter(downstream, connect_rate, lw=1, color='red', label="报警关联", marker='*')
    ax2.scatter(length, connect_rate, lw=1, color='red', label="报警关联", marker='^')
    plt.show()

    ax = plt.subplot(111, projection='3d')  # 创建一个三维的绘图工程
    #  将数据点分成三部分画，在颜色上有区分度
    ax.scatter(downstream, length, connect_rate, c='y')  # 绘制数据点
    ax.set_zlabel('报警关联度')  # 坐标轴
    ax.set_ylabel('路段长度')
    ax.set_xlabel('下游报警')
    plt.show()


def alarm_reason(alarm_data):
    rdsect_list = (alarm_data['rdsectid'].drop_duplicates())
    # print(rdsect_list)
    rdsect_dict = []
    for i in range(len(rdsect_list)):
        k = 0
        reason = 'demo'
        cong = 'demo'
        rdsectid = rdsect_list.iloc[i]
        match_data = alarm_data[alarm_data['rdsectid'] == rdsectid]
        alarm_time = match_data['time_point']
        veh_dir = match_data['vehicle_dir']
        delay_value = match_data['delay_value']
        length = match_data.iloc[0][8]
        # int_name = match_data
        rdsect_neme = match_data.iloc[0][7]
        list_entrance = []
        list_exit = []
        time_entrace = []
        time_exit = []
        for j in range(len(veh_dir)):
            # print((veh_dir.iloc[j])[1:])
            if (veh_dir.iloc[j])[1:] == '进口':
                a = delay_value.iloc[j]
                b = None
                time_entrace.append(alarm_time.iloc[j])
            else:
                a = None
                b = delay_value.iloc[j]
                time_exit.append(alarm_time.iloc[j])
            list_entrance.append(a)
            list_exit.append(b)
        alarm_times_ent = len(time_entrace)
        alarm_times_exit = len(time_exit)
        connect = []
        for m in range(len(time_exit)):
            score = 0
            reservoir = 0
            for n in range(len(time_entrace)):
                # 判断报警关联性
                t0 = dt.datetime.strptime(str(time_exit[m]), '%H:%M:%S')
                t1 = dt.datetime.strptime(str(time_entrace[n]), '%H:%M:%S')
                if dt.datetime.strptime(str(time_exit[m]), '%H:%M:%S') - dt.timedelta(
                        seconds=900) <= dt.datetime.strptime(str(time_entrace[n]), '%H:%M:%S') \
                        <= dt.datetime.strptime(str(time_exit[m]), '%H:%M:%S') + dt.timedelta(seconds=0):
                    reservoir += 1
                    if t0-t1 == dt.timedelta(seconds=0):
                        score += 10
                    elif t0-t1 == dt.timedelta(seconds=120):
                        score += 8
                    elif t0-t1 == dt.timedelta(seconds=240):
                        score += 6
                    elif t0-t1 == dt.timedelta(seconds=360):
                        score += 4
                    elif t0-t1 == dt.timedelta(seconds=480):
                        score += 2
            if (m >= 1 and dt.datetime.strptime(str(time_exit[m]), '%H:%M:%S') - dt.datetime.strptime(
                    str(time_exit[m - 1]), '%H:%M:%S') > dt.timedelta(seconds=360)) or m == 0:
                if score >=14:
                    reason = "通行能力不足"
                else:
                    reason = "排队空间不足"
                k += 1
                    # 判断上游报警前三个周期内无报警


                # for o in range(n):
                #     # 判断关联报警前5个周期内下游报警次数，若下游报警次数大于等于2，
                #     # 说明该路段拥堵从上游蔓延到下游至少经过了3个周期，排队空间充足
                #     if dt.datetime.strptime(str(time_exit[m]), '%H:%M:%S')-dt.timedelta(seconds=1200) \
                #             <= dt.datetime.strptime(str(time_entrace[o]), '%H:%M:%S')\
                #             < dt.datetime.strptime(str(time_exit[m]), '%H:%M:%S')-dt.timedelta(seconds=120):
                #         reservoir += 1
                #
                # cong = length/(reservoir*2)
                # if reservoir >= 3:
                #     reason = '通行能力不足'
                # else:
                #     reason = '排队空间不足'
            connect.append([time_exit[m], score, reason, cong, reservoir])
            # break

        rdsect_dict.append([rdsectid, alarm_times_ent, alarm_times_exit, k, int(length), connect])
    print(rdsect_dict)
    return rdsect_dict


def k_means():
    # 生成10*3的矩阵
    data = np.random.rand(10, 3)
    # 聚类为4类
    estimator = KMeans(n_clusters=4)
    # fit_predict表示拟合+预测，也可以分开写
    res = estimator.fit_predict(data)
    # 预测类别标签结果
    lable_pred = estimator.labels_
    # 各个类别的聚类中心值
    centroids = estimator.cluster_centers_
    # 聚类中心均值向量的总和
    inertia = estimator.inertia_


def data_insert(data,date):
    try:
        conn = pg.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
    except Exception as e:
        print(e)
    else:
        cr_pg = conn.cursor()
        sql_insert = "insert into alarm_connect values (%s,%s,%s,%s,%s)"
        sql_insert2 = "insert into alarm_connect_reson2 values (%s,%s,%s,%s,%s,%s,%s,%s)"
        for i in data:
            try:
                cr_pg.execute(sql_insert, (i[0],i[1],i[2],i[3],i[4],))
                conn.commit()
            except Exception as e:
                print(e)
            for j in i[5]:
                cr_pg.execute(sql_insert2, (i[0],j[0],j[1],j[2],i[4],j[3],date,j[4]))
                conn.commit()
        conn.close()
        # tru2list = [list(i) for i in result]





def main():
    end_day = '2018-07-20'
    alarm_data = call_alarm_data('2018-07-20', end_day)
    # print(alarm_data)
    rdsect_dict = alarm_reason(alarm_data)
    # print(rdsect_dict)

    draw_alarm(alarm_data)
    # alarm_reason(alarm_data)
    # draw_alarm_connect(rdsect_dict)
    # data_insert(rdsect_dict,end_day)


if __name__ == '__main__':
    main()
