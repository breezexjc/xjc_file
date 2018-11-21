#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mar 16 2018
Description:
@author:  dcy
"""
import numpy as np
import psycopg2
import pandas as pd
import datetime as dt
import node_group_v2
import node_group_v3
import GlobalContent
import alarm_strategy_input
import group_popup_info
import alarm_group_page
import cx_Oracle
import requests
import json


class Constants:
    time_delta = 240  # seconds
    start_date = '2018-7-18'
    end_date = '2018-7-18'
    start_time = '08:00:00'
    end_time = '09:00:00'
    workdays = [0, 1, 2, 3, 4]
    alarm_holidays = 'alarm_holidays'


# 获取报警原始数据
def call_postgres(conn, start_date,  start_time, end_time):
    match_roadsect_info = pd.DataFrame({})  # 存放路段信息
    match_alarm_records = pd.DataFrame({})  # 存放报警数据
    try:  # 数据库连接超时
        cr = conn.cursor()
        # 路段，上下游节点，进出口道信息
        sql1 = "select * from {0}".format(GlobalContent.alarm_rdsect)
        cr.execute(sql1)
        rs1 = cr.fetchall()
        try:
            match_roadsect_info = pd.DataFrame(rs1)
            match_roadsect_info.columns = ['rdsectid', 'up_node', 'down_node', 'road_dir', 'dir_desc',
                                           'import_desc', 'export_desc', 'fstr_desc', 'length']
        except Exception as e:
            print('call_postgres', e)
        # 报警记录
        sql2 = "select a.scats_id, a.date_day, a.time_point, a.vehicle_dir , delay_value from {3} a where date_day " \
               " ='{0}' and time_point between '{1}' and '{2}' order by scats_id, date_day, time_point"\
               .format(start_date, start_time, end_time, GlobalContent.alarm_record)
        # print(sql2)
        cr.execute(sql2)
        rs2 = cr.fetchall()

        try:
            match_alarm_records = pd.DataFrame(rs2)
            match_alarm_records.columns = ['scats_id', 'date_day', 'time_point', 'vehicle_dir', 'delay_value']
        except Exception as e:
            print('call_postgres', e)
        cr.close()
    except Exception as e:
        print(e)

    return match_roadsect_info, match_alarm_records


# # 调用API判断节假日
# def isHoliday(start_date, end_date):
#     holidays = []
#     workdays = []
#     content = {'pageIndex': 1, 'pageSize': 10, 'categoryId': 9}
#     start_date_dt = dt.datetime.strptime(start_date, '%Y-%m-%d')
#     end_date_dt = dt.datetime.strptime(end_date, '%Y-%m-%d')
#     time_list = pd.date_range(start_date_dt, end_date_dt, freq='86400s')
#     for i in time_list:
#         date = i.to_pydatetime()
#         date = dt.datetime.strftime(date, '%Y-%m-%d')
#         date_change = date[0:4]+date[5:7]+date[8:]
#         r = requests.get(url='http://api.goseek.cn/Tools/holiday?date={0}'.format(date_change), params=content)
#         get_date = r.json()
#         # json_data = get_date.json()
#         print(type(get_date))
#         if get_date['data'] == 0:
#             workdays.append(date)
#         elif get_date['data'] == 1 or get_date['data'] == 2:
#             holidays.append(date)
#         else:
#             print('返回错误数据', get_date['data'])
#     print('workdays:', workdays, '\n', 'holidays:', holidays)
#     return workdays, holidays


# 休息日数据获取
def call_holiday(conn, start_date, end_date):
    holidays_data = []
    workday = []
    holiday = []
    sql_select = "select * from {0} where date between to_date('{1}','yyyy-MM-dd') and to_date('{2}','yyyy-MM-dd')"\
                 .format(Constants.alarm_holidays, start_date, end_date)

    try:
        # conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
        #                         user=GlobalContent.pg_database72_research['user'],
        #                         password=GlobalContent.pg_database72_research['password'],
        #                         host=GlobalContent.pg_database72_research['host'],
        #                         port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
        cr.execute(sql_select)
    except Exception as e:
        print('call_holiday', e)
    else:
        holidays_data = cr.fetchall()
        cr.close()
        if len(holidays_data) > 0:
            # holiday = [list(i) for i in holidays_data]
            for i in holidays_data:
                if i[1] == '休息日':
                    holiday.append(i[0])
                elif i[1] == '工作日':
                    workday.append(i[0])
                else:
                    pass

        else:
            print('无法获取节假日数据')
            pass
    return holiday, workday


# 获取节假日列表失败时，按双休日判断
def days_select(isworkday, date_list):
    holidays = []
    workdays = []
    days0 = 0
    days1 = 1
    # 获取数据中包含的日期
    for date in date_list:
        # date = i.to_pydatetime()
        if date.weekday() in Constants.workdays:
            days1 += 1
            workdays.append(date)
        else:
            days0 += 1
            holidays.append(date)
    alldays = holidays + workdays

    if isworkday == 0:
        print('holidays:', holidays)
        return days0, holidays
    elif isworkday == 1:
        print('workdays:', workdays)
        return days1, workdays
    elif isworkday == 2:
        print('alldays:', alldays)
        return days0+days1, alldays
    else:
        print('日期选择指令异常，应该为int类型的0,1,2')
        return 0,  []


def time_to_datetime(time):
    hour = time.hour
    minute = time.minute
    datetime = dt.datetime(2018, 3, 16, hour, minute, 0)
    return datetime


# 上游自下游的报警关联匹配
def up_to_down(up_node_data, down_node_data, match_num, match_data1, up_match_num):
    time_up = []
    time_down = []
    all_score = 0
    per_day_match_num = 0
    print(up_node_data)
    print(len(up_node_data))
    for p in range(len(up_node_data)):
        print("start")
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
        print(t0, score)
        all_score += score
        if score >= 16:
            # 匹配次数加1
            per_day_match_num += 1
            if t0 not in time_up:
                up_delay = up_node_data.iloc[p]['delay_value']
                match_data1.append(up_delay)
                match_data1 = match_data1+m1
                time_up.append(t0)
                time_down = time_down+ts

    up_match_num.append(per_day_match_num)

    return match_data1, up_match_num, time_up, all_score


# 下游自上游的报警关联匹配
def down_to_up(up_node_data, down_node_data, match_num2, match_data2, down_match_num):
    time_up = []
    time_down = []
    per_day_match_num = 0
    for p in range(len(down_node_data)):
        t0 = time_to_datetime(down_node_data.iloc[p]['time_point'])  # time转成datetime
        for q in range(len(up_node_data)):
            t1 = time_to_datetime(up_node_data.iloc[q]['time_point'])
            if t0 <= t1 and t1 - t0 <= dt.timedelta(seconds=Constants.time_delta):
                match_num2 += 1
                per_day_match_num += 1
                if t0 not in time_up:
                    up_delay = up_node_data.iloc[q]['delay_value']
                    match_data2.append(up_delay)
                if t1 not in time_down:
                    down_delay = down_node_data.iloc[p]['delay_value']
                    match_data2.append(down_delay)
                time_up.append(t0)
                time_down.append(t1)
                break
    down_match_num.append(per_day_match_num)
    return match_data2, down_match_num


# 路口平均报警次数
def int_alarm_times(scats_id):
    int_alarm = grouped_alarms.get_group(scats_id)
    avg_alarm_time = round(len(int_alarm))
    return avg_alarm_time


# 匹配数据中的日期
def days_match(isworkday,holiday, workday, date_list):
    workdays = []
    holidays = []
    days0 = 0
    days1 = 0
    for i in date_list:
        if i in workday:
            workdays.append(i)
            days1 += 1
        elif i in holiday:
            holidays.append(i)
            days0 += 1
        else:
            pass
    alldays = holidays + workdays
    if isworkday == 0:
        print('holidays:', holidays)
        return days0, holidays
    elif isworkday == 1:
        print('workdays:', workdays)
        return days1, workdays
    elif isworkday == 2:
        print('alldays:', alldays)
        return days0+days1, alldays
    else:
        print('日期选择指令异常，应该为int类型的0,1,2')
        return 0,  []


def alarm_match_data_send(conn, start_date, start_time, end_time,  match_num_list, match_num_list2):
    sql_delete = "delete from {0} where date_day = '{1}' and time_interval ='{2}'" \
        .format(GlobalContent.alarm_match_data, start_date, start_time + '~' + end_time)

    sql = "INSERT INTO {0} (rdsectid, up_to_down_match_num, date_day, down_to_up_match_num,time_interval, " \
          "forward_avg_delay,backward_avg_delay, length,score)" \
          " VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)" \
        .format(GlobalContent.alarm_match_data)
    # sql_delete = "delete from " + GlobalVar.PostgresqlTableName + " where item_date = '" + str_date + \
    #              "' and intersect_id = '" + str_int + "' and data_type = '" + data_type + "'"
    cur = conn.cursor()
    try:
        cur.execute(sql_delete)
        conn.commit()
        print('清除已有数据')
    except Exception as e:
        print('main_sql_delete', e)
    for m in range(len(match_num_list)):
        try:
            # print(match_num_list[m][0] + match_num_list2[m][0])
            # print(match_num_list[m][0] + match_num_list2[m][0])
            if match_num_list[m][0] > 0:
                # data = (result[m][0], result[m][1], str_date)
                print('数据插入成功')
                cur.execute(sql, (match_num_list[m][4], (match_num_list[m][0]), start_date ,
                                  None, start_time + '~' + end_time, match_num_list[m][1],
                                  None, int(match_num_list[m][2]), match_num_list[m][3]))

                conn.commit()
        except psycopg2.IntegrityError:
            print('数据重复')
            conn.commit()
            # cur.execute(sql_delete)
    conn.commit()
    cur.close()


# 主程序
def main(cal_date,  start_time, end_time, isworkday):
    print(cal_date,  start_time, end_time)

    global grouped_alarms
    rds_id = []
    try:
        # 链接PG数据库
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
    except Exception as e:
        print("conn:", e)
    else:
        # 确定工作日和休息日
        # holiday, workday = call_holiday(conn, start_date, end_date)
        # 获取道路信息和报警数据
        match_roadsect_info, match_alarm_records = call_postgres(conn, cal_date, start_time, end_time)
        print(match_alarm_records)

        match_num_list = []  # up_to_down
        match_num_list2 = []  # down_to_up
        # 匹配报警次数
        # print(match_alarm_records)
        if not match_alarm_records.empty:
            grouped_alarms = match_alarm_records.groupby(['scats_id'])  # 报警记录按路口分组
            rds_id = match_roadsect_info['rdsectid'].tolist()
            # print(days, match_date)
            for m in range(len(match_roadsect_info)):
                rdsectid = rds_id[m]
                up_match_num = []
                match_num = 0  # 每条记录匹配次
                match_data1 = []
                avg_delay1 = 0
                # print('import' + str(import_desc))
                # print('export' + str(export_desc))
                all_score = 0
                try:
                    # 上下游节点所有报警记录选择
                    up_node_data_all = grouped_alarms.get_group(match_roadsect_info.iloc[m]['up_node'])
                    down_node_data_all = grouped_alarms.get_group(match_roadsect_info.iloc[m]['down_node'])
                except Exception as e:
                    pass
                else:
                    # 上下游均有报警记录
                    length = match_roadsect_info.iloc[m]['length']
                    import_desc = match_roadsect_info.iloc[m]['import_desc']
                    export_desc = match_roadsect_info.iloc[m]['export_desc']

                    # 报警记录按方向分组
                    grouped_up_alarms_dir = up_node_data_all.groupby(['vehicle_dir'])
                    grouped_down_alarms_dir = down_node_data_all.groupby(['vehicle_dir'])
                    # 上下游节点带方向报警记录
                    # print(up_node_data_all)
                    # print(down_node_data_all)
                    try:
                        up_node_data = grouped_up_alarms_dir.get_group(export_desc)
                        down_node_data = grouped_down_alarms_dir.get_group(import_desc)
                        # print(up_node_data)
                        # print(down_node_data)

                    except Exception as e:
                        print(e)
                        pass
                    else:
                        # 匹配
                        print("匹配成功")
                        match_data1, up_match_num, time_up, all_score = up_to_down(up_node_data, down_node_data,
                                                                                   match_num, match_data1, up_match_num)
                        # print(all_score)
                        if len(up_match_num) > 0:
                            match_num = sum(up_match_num)
                        else:
                            match_num = 0
                        avg_delay1 = avg_list(match_data1)
                        match_num_list.append([match_num, avg_delay1, length, all_score,rdsectid])
                        # print(match_num)
                    # match_num_list.append([match_num, avg_delay1, length])
        # print('match_num_list:', match_num_list)
        # print('match_num_list:', match_num_list2)
        # print(match_num_list)
        # 匹配结果发送
        alarm_match_data_send(conn, cal_date, start_time, end_time, match_num_list, match_num_list2)
        # 关闭数据库链接
        conn.close()
        # print(date_interval, time_interval)
        # 拥堵组团分析，页面数据生成
        node_group_v3.main(cal_date, start_time, end_time)
        # int_order = group_popup_info.main(cal_date, start_time, end_time)
        try:
            # alarm_group_page.main(cal_date, cal_date, start_time, end_time, int_order)
            print("绘图数据已发送")
        except Exception as e:
            print('alarm_group_page')
            print(e)
        # 生成调控建议
        # alarm_strategy_input.main(cal_date, start_time, end_time, isworkday)
        # 生成弹窗基础数据

        return


# 求列表平均值
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


if __name__ == '__main__':
    main(Constants.start_date, Constants.start_time, Constants.end_time, 2)


