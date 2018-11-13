#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mar 21 2018
Description:
@author:  xjc
"""
import psycopg2
import pandas as pd
import datetime as dt
import matplotlib
import matplotlib.pyplot as plt


def call_postgres(start_date, end_date, start_time, end_time):
    match_roadsect_info = pd.DataFrame({})
    match_alarm_records = pd.DataFrame({})
    try:  # 数据库连接超时
        conn = psycopg2.connect(database="superpower", user="postgres", password="postgres",
                                host="192.168.22.72", port="5432")
        cr = conn.cursor()
        # 路段，上下游节点，进出口道信息
        sql1 = "select * from alarm_rdsect_desc_select_desc"
        cr.execute(sql1)
        rs1 = cr.fetchall()
        match_roadsect_info = pd.DataFrame(rs1)
        match_roadsect_info.columns = ['rdsectid', 'up_node', 'down_node', 'road_dir', 'dir_desc',
                                       'import_desc', 'export_desc', 'fstr_desc']
        # 报警记录
        sql2 = "select a.scats_id, a.date_day, a.time_point, a.vehicle_dir, a.delay_value from alarm_record a where date_day " \
               "between '{0}' and '{1}' and time_point between '{2}' and '{3}' order by scats_id, date_day, time_point"\
            .format(start_date, end_date, start_time, end_time)
        cr.execute(sql2)
        rs2 = cr.fetchall()
        match_alarm_records = pd.DataFrame(rs2)
        match_alarm_records.columns = ['scats_id', 'date_day', 'time_point', 'vehicle_dir','delay_value']
        conn.close()
    except Exception as e:
        print(e)

    return match_roadsect_info, match_alarm_records


def count_alarm(alarm_records):
    a1 = alarm_records[alarm_records['delay_value'] < 30]
    a2 = alarm_records[(alarm_records['delay_value'] >= 30) & (alarm_records['delay_value'] <= 60)]
    a3 = alarm_records[(alarm_records['delay_value'] >= 60) & (alarm_records['delay_value'] <= 100)]
    x1 = a1.size
    x2 = a2.size
    x3 = a3.size
    data = [x1, x2, x3]
    xticks = [1, 2, 3]
    bars = plt.bar(xticks, data, width=0.5, edgecolor='none')
    plt.show()


def main(start_date, end_date, start_time, end_time):
    match_roadsect_info, match_alarm_records = call_postgres(start_date, end_date, start_time, end_time)
    count_alarm(match_alarm_records)


main('2018-04-26', '2018-5-10', '08:00:00' , '09:00:00')