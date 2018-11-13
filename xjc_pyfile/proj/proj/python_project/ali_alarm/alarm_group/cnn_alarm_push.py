import psycopg2 as pg
from ali_alarm import GlobalContent
import time
import pandas as pd
import datetime as dt
import numpy as np


def call_rdsect_inf():
    try:
        conn = pg.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
    except Exception as e:
        print(e)
    else:
        match_roadsect_info = []
        cr = conn.cursor()
        sql1 = "select * from {0}".format(GlobalContent.alarm_rdsect)
        cr.execute(sql1)
        rs1 = cr.fetchall()
        try:
            match_roadsect_info = pd.DataFrame(rs1)
            match_roadsect_info.columns = ['rdsectid', 'up_node', 'down_node', 'road_dir', 'dir_desc',
                                           'import_desc', 'export_desc', 'fstr_desc', 'length']
        except Exception as e:
            print('call_postgres', e)
        finally:
            conn.close()
            return match_roadsect_info


def get_data(start_date,start_time,end_time):
    try:
        conn = pg.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
    except Exception as e:
        print(e)
    else:
        result = []
        try:
            cr = conn.cursor()
            sql2 = "select a.scats_id, a.date_day, a.time_point, a.vehicle_dir , delay_value from {3} a where date_day " \
                   " = '{0}'  and time_point between '{1}' and '{2}' order by scats_id, date_day, time_point" \
                .format(start_date, start_time, end_time, GlobalContent.alarm_record)
            cr.execute(sql2)
            rs2 = cr.fetchall()
            match_alarm_records = pd.DataFrame(rs2)
            match_alarm_records.columns = ['scats_id', 'date_day', 'time_point', 'vehicle_dir', 'delay_value']
        except Exception as e:
            pass
        finally:
            conn.close()
        return match_alarm_records


def real_main():
    rdsect_inf = call_rdsect_inf()
    while True:
        date = []
        localtime = dt.datetime.now()
        date_day = localtime.date()
        date_day = '2018-07-02'
        end_time = localtime.time()
        print(str(localtime)[11:19])
        start_time = localtime-dt.timedelta(seconds=600)
        # print(date_day)
        date = get_data(date_day, start_time, end_time)
        # print(date)
        # print(rdsect_inf)
        match_result = cnn_alarm_match(date, rdsect_inf)
        for i in match_result:
            print(i)
        print('_______________________________________________\n')
        time.sleep(60)


def avg_list(data):
    sum = 0
    len_data = len(data)
    if len_data > 0 :
        for i in data:
            sum += i
        avg = sum/len_data
        return avg
    else:
        return 0


def cnn_alarm_match(match_alarm_records, match_roadsect_info):
    match_num_list = []  # up_to_down
    match_num_list2 = []  # down_to_up

    if not match_alarm_records.empty:
        grouped_alarms = match_alarm_records.groupby(['scats_id'])  # 报警记录按路口分组
        rds_id = match_roadsect_info['fstr_desc'].tolist()
        down_node = match_roadsect_info['down_node'].tolist()
        # print(days, match_date)
        for m in range(len(match_roadsect_info)):
            time_up = []
            all_score = []
            up_match_num = 0
            # print('路段' + str(match_roadsect_info.iloc[m]['rdsectid']))
            # print('up_node' + str(match_roadsect_info.iloc[m]['up_node']))
            # print('down_node' + str(match_roadsect_info.iloc[m]['down_node']))
            match_num = 0  # 每条记录匹配次
            match_data1 = []
            avg_delay1 = 0
            # 进出口道信息获取
            length = match_roadsect_info.iloc[m]['length']
            import_desc = match_roadsect_info.iloc[m]['import_desc']
            export_desc = match_roadsect_info.iloc[m]['export_desc']
            try:
                # 上下游节点所有报警记录
                up_node_data_all = grouped_alarms.get_group(match_roadsect_info.iloc[m]['up_node'])
                down_node_data_all = grouped_alarms.get_group(match_roadsect_info.iloc[m]['down_node'])
                # 按日期分组
                up_node_data_byday = up_node_data_all
                down_node_data_byday = down_node_data_all

                try:
                    up_node_data_1day = up_node_data_byday
                    down_node_data_1day = down_node_data_byday
                    # print(up_node_data_1day)
                    # 报警记录按方向分组
                    grouped_up_alarms_dir = up_node_data_1day.groupby(['vehicle_dir'])
                    grouped_down_alarms_dir = down_node_data_1day.groupby(['vehicle_dir'])
                    # 上下游节点带方向报警记录
                    up_node_data = grouped_up_alarms_dir.get_group(export_desc)
                    down_node_data = grouped_down_alarms_dir.get_group(import_desc)
                    # print(up_node_data)
                    # print(down_node_data)
                    # 匹配
                    match_data1, up_match_num, time_up, all_score = up_to_down(up_node_data, down_node_data,
                                                                               match_num, match_data1,
                                                                               up_match_num)
                except KeyError as e:
                    pass
                if up_match_num> 0:
                    match_num = up_match_num
                else:
                    match_num = 0
                    match_num2 = 0
                avg_delay1 = avg_list(match_data1)
                # avg_delay2 = avg_list(match_data2)
                if match_num>0:
                    match_num_list.append([down_node[m],rds_id[m], time_up,all_score ,length])
                # match_num_list2.append([match_num2, avg_delay2, length])
            except KeyError as e:
                pass
                # print(e, '无报警记录')
                # match_num_list.append([match_num, avg_delay1, length,time_up, all_score])
    return match_num_list


def time_to_datetime(time):
    hour = time.hour
    minute = time.minute
    datetime = dt.datetime(2018, 3, 16, hour, minute, 0)
    return datetime


def up_to_down(up_node_data, down_node_data, match_num, match_data1, up_match_num):
    time_up = []
    time_down = []
    all_score = []
    per_day_match_num = 0
    # print(up_node_data)
    # print(len(up_node_data))
    for p in range(len(up_node_data)):
        # print("start")
        m1 = []  # 存放临时delay值
        ts = []
        t0 = time_to_datetime(up_node_data.iloc[p]['time_point'])  # time转成datetime
        score = 0
        for q in range(len(down_node_data)):
            t1 = time_to_datetime(down_node_data.iloc[q]['time_point'])
            if t0-t1 == dt.timedelta(seconds=0):
                score += 10
                if t1 not in time_down:
                    down_delay = down_node_data.iloc[q]['delay_value']
                    m1.append(down_delay)
                    ts.append(t1)
            elif t0-t1 == dt.timedelta(seconds=120):
                score += 8
                if t1 not in time_down:
                    down_delay = down_node_data.iloc[q]['delay_value']
                    m1.append(down_delay)
                    ts.append(t1)
            elif t0 - t1 == dt.timedelta(seconds=240):
                score += 6
                if t1 not in time_down:
                    down_delay = down_node_data.iloc[q]['delay_value']
                    m1.append(down_delay)
                    ts.append(t1)
            elif t0 - t1 == dt.timedelta(seconds=360):
                score += 4
                if t1 not in time_down:
                    down_delay = down_node_data.iloc[q]['delay_value']
                    m1.append(down_delay)
                    ts.append(t1)
            elif t0 - t1 == dt.timedelta(seconds=480):
                score += 2
                if t1 not in time_down:
                    down_delay = down_node_data.iloc[q]['delay_value']
                    m1.append(down_delay)
                    ts.append(t1)
            else:
                pass
        # print(t0, score)
        all_score.append(score)
        if score >= 15:
            # 匹配次数加1
            reason = "通行能力不足"
            per_day_match_num += 1
            if t0 not in time_up:
                up_delay = up_node_data.iloc[p]['delay_value']
                match_data1.append(up_delay)
                match_data1 = match_data1+m1
                time_up.append(str(t0)[11:])
                time_down = time_down+ts
            up_match_num = per_day_match_num
        else:
            reason = "排队空间不足"
    # print('-------------------')
    # print(up_match_num)
    return match_data1, up_match_num, time_up, all_score


real_main()