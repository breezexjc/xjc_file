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
import numpy as np
import GlobalContent
import alarm_group_page
import cx_Oracle
import time
import json
from collections import Counter


class CONTENT():
    lang = GlobalContent.lang

# 获取协调方向信息
def call_coor_dir(conn, start_time, end_time, node_inf):

    sql = "select * from (SELECT c.start_time,c.end_time,c.upstream_nodeid,c.downstream_nodeid,c.segment_dir,c.upstream_phasename," \
          "C.dowmstream_phasename,d.rdsectid from" \
          "(SELECT a.*,b.segment_dir " \
          "FROM(SELECT k.start_time,k.end_time,k.route_id,k.segment_id,k.route_name,t.upstream_nodeid," \
          "t.downstream_nodeid,t.upstream_phasename,T.dowmstream_phasename " \
          "FROM(select m.start_time,m.end_time,m.route_id,m.route_name,n.segment_id " \
          "from coor_route m ,coor_route_segment_rel n where m.route_id = n.route_id) K,coor_phase_diff t " \
          "where k.segment_id=t.segment_id) a LEFT JOIN coor_route_segment b on a.segment_id = b.segment_id) c" \
          "	left join pe_tobj_roadsect d on c.upstream_nodeid=d.new_up_nod and c.downstream_nodeid=d.new_down_n " \
          "ORDER BY c.route_id)f where f.rdsectid is not null"
    # date_interval = start_date + '~' + end_date
    # time_interval = start_time + '~' + end_time
    coor_dir = pd.DataFrame({})
    try:  # 数据库连接超时
        # conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
        #                         user=GlobalContent.pg_database72_research['user'],
        #                         password=GlobalContent.pg_database72_research['password'],
        #                         host=GlobalContent.pg_database72_research['host'],
        #                         port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
        cr.execute(sql)
        rows = cr.fetchall()
        # print(rows)
        rows_list = []
        for i in rows:
            i = list(i)
            i[0] = dt.time.strftime(i[0], '%H:%M:%S')
            i[1] = dt.time.strftime(i[1], '%H:%M:%S')
            for j in node_inf:
                if j[0] == i[2]:
                    i[2] = j[1]
                if j[0] == i[3]:
                    i[3] = j[1]
            rows_list.append(i)
        # print(rows)
        coor_dir = pd.DataFrame(rows_list)
        coor_dir.columns = ['start_time', 'end_time', 'up_node', 'down_node', 'coor_dir', 'up_phase','down_phase', 'rdsectid']
        cr.close()

    except Exception as e:
        print(e)

    match_coor = coor_dir[(((coor_dir['start_time'] > start_time) & (coor_dir['start_time'] < end_time)) |
                          ((coor_dir['end_time'] > start_time) & (coor_dir['end_time'] < end_time)) |
                          ((coor_dir['start_time'] < start_time) & (coor_dir['end_time'] > end_time)))]

    # print(frame)
    return match_coor


# 获取节点配置信息
def call_node_inf():
    sql = "select nodeid,systemid from pe_tobj_node "
    rows_list = []
    try:  # 数据库连接超时
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
        cr.execute(sql)
        rows = cr.fetchall()
        rows_list = []
        for i in rows:
            i = list(i)
            rows_list.append(i)
        # print(rows_list)
        cr.close()
        conn.close()
    except Exception as e:
        print(e)
    # print(frame)
    return rows_list


# 获取各进口道报警次数
def call_alarm_times(conn, start_date,  start_time, end_time):
    alarm_times = []
    sql = "SELECT date_day,scats_id,vehicle_dir,count(scats_id) from {0} where date_day ='{1}' " \
          "and time_point BETWEEN '{2}' and '{3}' GROUP BY date_day,vehicle_dir,scats_id " \
          "ORDER BY date_day,scats_id".format(GlobalContent.alarm_record, start_date, start_time, end_time)
    try:
        # conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
        #                         user=GlobalContent.pg_database72_research['user'],
        #                         password=GlobalContent.pg_database72_research['password'],
        #                         host=GlobalContent.pg_database72_research['host'],
        #                         port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
    except Exception as e:
        print('call_holiday', e)
    else:
        cr.execute(sql)
        alarm_times = cr.fetchall()
        # frame = pd.DataFrame(alarm_times)
        # frame.columns = ['date', 'scats_id', 'veh_desc', 'alarm_times']
        conn.commit()
        cr.close()
    return alarm_times


# 报警次数匹配
def alarm_times_match(alarm_times):
    # print(date_list)
    # for i in range(len(date_list)):
    #     date_list[i] = str(date_list[i])
    # print(alarm_times)
    match_date_grouped = []
    alarm_times_match = []
    for i in alarm_times:
        # if i[0] in date_list:
        alarm_times_match.append(i)

    if len(alarm_times_match) > 0:
        match_date = pd.DataFrame(alarm_times_match)
        match_date.columns = ['date', 'scats_id', 'veh_desc', 'alarm_times']
        scats_id = match_date['scats_id'].drop_duplicates().tolist()
        scats_id = sorted(scats_id)
        match_date_grouped = match_date.groupby(['scats_id', 'veh_desc'])['alarm_times'].sum()

    return match_date_grouped



def call_lane_inf(conn):
    sql ="SELECT scatsid,roadsectid,length1, sum(case when p.ln_function = '直行' then count_lane " \
         "when p.ln_function in('左直','直左','直右') then round(count_lane/2::NUMERIC,1) " \
         "when p.ln_function = '直左右' then round(count_lane/3::NUMERIC,2) end) as str_lane, " \
         "sum(case when p.ln_function = '左转' then count_lane " \
         "when p.ln_function in('左直','直左') then round(count_lane/2::NUMERIC,2) " \
         "when p.ln_function = '直左右' then round(count_lane/3::NUMERIC,2) end) as left_lane, " \
         "sum(case when p.ln_function = '右转' then count_lane " \
         "when p.ln_function in('直右') then round(count_lane/2::NUMERIC,2) " \
         "when p.ln_function = '直左右' then round(count_lane/3::NUMERIC,2) end) as right_lane " \
         "FROM(SELECT scatsid,roadsectid,ln_function,ln_location,length1,count(lanenum) as count_lane " \
         "FROM(SELECT M .scatsid,M .lanenum,M .lanetype,M .ln_function, M .ln_location,M .roadsectid,n.length1 " \
         "FROM(SELECT A .scatsid,A .lanenum,A .lanetype,A .la_fuction ln_function,REPLACE (b.place, '侧', '进口') " \
         "ln_location,b.roadsectid " \
         "FROM(SELECT scatsid,lanenum,lanetype,la_fuction,transectid " \
         "FROM pe_tobj_lane ORDER BY scatsid,lanenum) A LEFT JOIN pe_tobj_transect b ON A .transectid = b.transectid) M " \
         "LEFT JOIN pe_tobj_roadsect n ON M .roadsectid = n.rdsectid )o where  lanetype != '1003004' " \
         "group by scatsid,roadsectid,ln_function,ln_location,length1)p " \
         "GROUP BY scatsid,roadsectid,length1"
    try:  # 数据库连接超时
        # conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
        #                         user=GlobalContent.pg_database72_research['user'],
        #                         password=GlobalContent.pg_database72_research['password'],
        #                         host=GlobalContent.pg_database72_research['host'],
        #                         port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
    except Exception as e:
        print('call_alarm_rdsect', e)
    else:
        cr.execute(sql)
        rows = cr.fetchall()
        # if len(rows) > 0:
        cr.close()
        return rows


# 获取路段匹配信息
def call_alarm_rdsect(conn):
    rows_list = []
    match_roadsect_info = []
    try:  # 数据库连接超时
        # conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
        #                         user=GlobalContent.pg_database72_research['user'],
        #                         password=GlobalContent.pg_database72_research['password'],
        #                         host=GlobalContent.pg_database72_research['host'],
        #                         port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
    except Exception as e:
        print('call_alarm_rdsect', e)
    else:
        # 路段，上下游节点，进出口道信息
        sql = "select rdsectid,up_node,down_node,import_desc,export_desc,fstr_desc,length from {0}".format(GlobalContent.alarm_rdsect)
        cr.execute(sql)
        rows = cr.fetchall()
        if len(rows) > 0:
            match_roadsect_info = pd.DataFrame(rows)
            match_roadsect_info.columns = ['rdsectid', 'up_node', 'down_node',
                                           'import_desc', 'export_desc', 'fstr_desc', 'length']
        else:
            pass
        cr.close()
    return match_roadsect_info


# 获取组团节点信息
def call_group_node(conn, start_date, start_time, end_time):
    date_interval = start_date
    time_interval = start_time + '~' + end_time
    frame = pd.DataFrame({})
    rows_list = []
    sql = "select group_id ,scats_id,node_id,alarm_times from {0} where alarm_date = '{1}' and time_point ='{2}'"\
        .format(GlobalContent.alarm_group, date_interval, time_interval)
    try:  # 数据库连接超时
        # conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
        #                         user=GlobalContent.pg_database72_research['user'],
        #                         password=GlobalContent.pg_database72_research['password'],
        #                         host=GlobalContent.pg_database72_research['host'],
        #                         port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
        cr.execute(sql)
        rows = cr.fetchall()
        # print(rows)
        rows_list = []
        for i in rows:
            i = list(i)
            rows_list.append(i)
        cr.close()

    except Exception as e:
        print(e)
    # print(frame)
    return rows_list


# 获取组团路段信息
def call_group_rdsect(conn, start_date, start_time, end_time):
    date_interval = start_date
    time_interval = start_time + '~' + end_time
    frame = pd.DataFrame({})
    match_alarm_records = pd.DataFrame({})
    try:  # 数据库连接超时
        # conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'], user=GlobalContent.pg_database72_research['user'],
        #                         password=GlobalContent.pg_database72_research['password'], host=GlobalContent.pg_database72_research['host'],
        #                         port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
        sql = "select group_id, rdsectid, up_node, down_node from {0} where alarm_date = '{1}' and time_point ='{2}'"\
            .format(GlobalContent.alarm_group_rdsectid, date_interval, time_interval)
        cr.execute(sql)
        rows = cr.fetchall()
        frame = pd.DataFrame(rows)
        frame.columns = ['group_id', 'rdsectid', 'up_node', 'down_node']
        # print(frame)
        cr.close()

    except Exception as e:
        print('拥堵组团数据获取失败')
        print(e)

    return frame


# 相位匹配信息获取
def call_phase_match(conn):
    rows_list = []
    sql = "SELECT e.scats_id,e.node_id,e.phase_name,(case when e.functionid = '1' then '1' " \
          "when e.functionid = '2' then '2' when e.functionid = '3' then '3' when e.functionid = '5' then '1;3' " \
          "when e.functionid = '6' then '1;2' when e.functionid = '7' then '1;2;3' when e.functionid = '8' then '2' " \
          "when e.functionid = '9' then '2;3' end ) as functionid,e.roadsectid,e.import_desc, " \
          "(case when e.import_desc = '北进口' then (case when e.functionid = '1' then '南出口' " \
          "when e.functionid = '2' then '东出口' when e.functionid = '3' then '西出口' when e.functionid = '5' " \
          "then '南出口;西出口' when e.functionid = '6' then '南出口;东出口' " \
          "when e.functionid = '7' then '南出口;东出口;西出口' when e.functionid = '8' then '东出口;北出口' " \
          "when e.functionid = '9' then '东出口;西出口' end )when e.import_desc ='东进口' then " \
          "(case when e.functionid = '1' then '西出口' " \
          "when e.functionid = '2' then '南出口' when e.functionid = '3' then '北出口' " \
          "when e.functionid = '5' then '西出口;北出口'  when e.functionid = '6' then '西出口;南出口' " \
          "when e.functionid = '7'then '西出口;南出口;北出口' when e.functionid = '8' then '东出口;南出口' " \
          "when e.functionid = '9'then '北出口;南出口' end)when e.import_desc ='南进口' then " \
          "(case when e.functionid = '1' then '北出口' when e.functionid = '2' then '西出口' " \
          "when e.functionid = '3' then '东出口' when e.functionid = '5' then '北出口;东出口' " \
          "when e.functionid = '6' then '北出口;西出口' when e.functionid = '7' then '北出口;西出口;东出口' " \
          "when e.functionid = '8' then '南出口;西出口' when e.functionid = '9' then '西出口;东出口' end) " \
          "when e.import_desc ='西进口' then (case when e.functionid = '1' then '东出口' " \
          "when e.functionid = '2' then '北出口' when e.functionid = '3' then '南出口' " \
          "when e.functionid = '5' then '东出口;南出口' when e.functionid = '6' then '东出口;北出口' " \
          "when e.functionid = '7' then '东出口;北出口;南出口' when e.functionid = '8' then '北出口;西出口' " \
          "when e.functionid = '9' then '北出口;南出口' end) end) as flow_dir " \
          "FROM(SELECT distinct d.down_node as scats_id,d.node_id,d.phase_name,d.functionid,d.roadsectid,d.import_desc " \
          "FROM(SELECT c.down_node,b.*,c.import_desc " \
          "from(SELECT node_id,phase_name,lane_id,la_fuction,functionid,roadsectid " \
          "FROM {0} a ORDER BY node_id,phase_name ) b,alarm_rdsect c " \
          "where b.roadsectid=c.rdsectid) d ORDER BY scats_id,phase_name) e  " \
          "where e.functionid is not null"\
        .format(GlobalContent.alarm_phase_match)
    try:  # 数据库连接超时
        # conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
        #                         user=GlobalContent.pg_database72_research['user'],
        #                         password=GlobalContent.pg_database72_research['password'],
        #                         host=GlobalContent.pg_database72_research['host'],
        #                         port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
    except Exception as e:
        print('call_phase_match', e)
    else:
        cr.execute(sql)
        rows = cr.fetchall()
        for i in rows:
            i = list(i)
            rows_list.append(i)
        cr.close()
        # print(rows_list)

    return rows_list


# 休息日数据获取
def call_holiday(conn,start_date, end_date):
    holidays_data = []
    workday = []
    holiday = []
    sql_select = "select * from {0} where date between to_date('{1}','yyyy-MM-dd') and to_date('{2}','yyyy-MM-dd')"\
                 .format(GlobalContent.alarm_holidays, start_date, end_date)

    try:

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
def days_select(isholiday, date_list):
    holidays = []
    workdays = []
    days0 = 0
    days1 = 1
    # 获取数据中包含的日期
    for date in date_list:
        # date = i.to_pydatetime()
        if date.weekday() in GlobalContent.workdays:
            days1 += 1
            workdays.append(date)
        else:
            days0 += 1
            holidays.append(date)
    alldays = holidays + workdays

    if isholiday == 0:
        print('holidays:', holidays)
        return days0, holidays
    elif isholiday == 1:
        print('workdays:', workdays)
        return days1, workdays
    elif isholiday == 2:
        print('alldays:', alldays)
        return days0 + days1, alldays
    else:
        print('日期选择指令异常，应该为int类型的0,1,2')
        return 0, []


# 匹配车流方向对应相位
def flow_match(input_data, new_phase_match_data, new_lane_inf):
    match_data = []
    lane_inf = []
    # print('输入：', input_data)
    for i in range(len(input_data)):
        # print(input_data)
        # 获取拥堵进出口描述，及主要车流方向
        scats_id = input_data[i][0][0]
        veh_desc = input_data[i][0][1]
        rdsectid = input_data[i][0][2]
        alarm_times = input_data[i][0][3]
        functionid = input_data[i][1]
        phase_inf = phase_inf_combine(scats_id, new_phase_match_data)
        lane_inf = new_lane_inf
        # print(phase_inf)
        # print(lane_inf)

        rdsectid_import = 'default'
        count_lane = 0
        length = 0
        # for j in range(len(phase_inf)):
        #     if phase_inf[j][2] in functionid and (veh_desc == phase_inf[j][3] or veh_desc == phase_inf[j][4]):
        #         rdsectid_import = lane_inf[j][5]
        #     else:
        #         pass
        # if rdsectid_import != 'default':
        #     for j in  range(len(lane_inf)):
        #         if lane_inf[j][1] == rdsectid_import:
        # print(count_lane)
        # print(length)

        # 匹配主要车流对应相位
        for j in range(len(phase_inf)):
            # print(veh_desc, functionid)
            # print(phase_inf[j][3], phase_inf[j][2])
            # 进口道相位匹配
            if veh_desc == phase_inf[j][3] and functionid[0] in phase_inf[j][2]:
                match_phase = phase_inf[j][1]
                match_rdsectid = phase_inf[j][5]
                # print('匹配相位：',match_phase)
                for m in range(len(lane_inf)):
                    if lane_inf[m][1] == match_rdsectid:
                        if '1' in phase_inf[j][2]:
                            count_lane = lane_inf[m][3]
                        if '2' in phase_inf[j][2]:
                            count_lane = lane_inf[m][4]
                        if '3' in phase_inf[j][2]:
                            count_lane = lane_inf[m][5]
                        length = lane_inf[m][2]
                    else:
                        pass
                demand_value = demand_value_cal(alarm_times, count_lane, length)
                if demand_value > 0:
                    match_data.append([scats_id, match_phase, phase_inf[j][3],  functionid[0], round(demand_value,2)])
            # 出口道相位匹配
            if veh_desc in phase_inf[j][4]:
                index = phase_inf[j][4].index(veh_desc)
                if functionid[0] == phase_inf[j][2][index]:
                    match_phase = phase_inf[j][1]
                    match_rdsectid = phase_inf[j][5]
                    # print('匹配相位：', match_phase)
                    veh_desc_import = phase_inf[j][3]
                    for m in range(len(lane_inf)):
                        if lane_inf[m][1] == match_rdsectid:
                            if '1' in phase_inf[j][2]:
                                count_lane = lane_inf[m][3]
                            if '2' in phase_inf[j][2]:
                                count_lane = lane_inf[m][4]
                            if '3' in phase_inf[j][2]:
                                count_lane = lane_inf[m][5]
                            length = lane_inf[m][2]
                        else:
                            pass
                    if alarm_times > 0:
                        alarm_times = -alarm_times
                    demand_value = demand_value_cal(alarm_times, count_lane, length)
                    # print(demand_value)
                    match_data.append([scats_id, match_phase, phase_inf[j][3], functionid[0], round(demand_value, 2)])
    # print('输出：',match_data)

    return match_data


def reduce_list(phase_match):
    new_list = []
    for i in phase_match:
        if i not in new_list:
            new_list.append(i)
    return new_list


# 相位建议融合
def phase_combine(phase_match):
    phase_setting = []
    # print(phase_match)
    new_phase_match = reduce_list(phase_match)
    # phase_match = sorted(phase_match)
    # print(new_phase_match)
    if len(new_phase_match) > 0:
        # frame = pd.DataFrame(phase_match)
        # frame.columns = ['scats_id', 'phase_name', 'veh_desc', 'lane_function', 'demand_value']
        # frame_grouped = frame.groupby(['scats_id', 'phase_name',  'veh_desc', 'lane_function'])
        # print(frame)
        # print(frame_grouped['demand_value'].sum())
        for i in range(len(new_phase_match)):
            phase_name = new_phase_match[i][1]
            veh_desc = new_phase_match[i][2]
            function = new_phase_match[i][3]
            demand_value = new_phase_match[i][4]
            if phase_name not in phase_setting:
                phase_setting.append(phase_name)
        phase_data = []
        for i in range(len(phase_setting)):
            phase_demand = 0
            veh_setting = []
            for j in range(len(new_phase_match)):
                if phase_setting[i] == new_phase_match[j][1]:
                    if [new_phase_match[j][2], new_phase_match[j][3]] not in veh_setting:
                        veh_setting.append([new_phase_match[j][2], new_phase_match[j][3]])
                    phase_demand += new_phase_match[j][4]
            phase_data.append([phase_setting[i], veh_setting, phase_demand])
        # print('调整相位：', phase_setting)
        # print(new_phase_match)
        phase_data=sorted(phase_data, key=lambda phase_data: abs(phase_data[2]), reverse=True)
        # print('融合结果1：', phase_data)
        # phase_data
        phase_data_copy = []
        [phase_data_copy.append(i) for i in phase_data]
        n = 0
        for i in range(len(phase_data)):
            phase_function = phase_data[i][1]
            for j in range(len(phase_data)):
                d = compare_list(phase_data[i][1], phase_data[j][1])
                if d == 1: # 第一个列表中元素都包含在第二个中
                    if abs(phase_data[i][2]) > abs(phase_data[j][2]):
                        # 去除同放
                        # phase_data[j][2] = 0
                        phase_data[j][2] = phase_data[j][2]-phase_data[i][2]
                        phase_data[j][1] = remove_list(phase_data[i][1], phase_data[j][1])


                    # for j in range(len(phase_data)):
                    #     # print(phase_data[j])
                    #     d = compare_list(phase_data[j][1], phase_data[i][1])
                    #     if len(phase_data[j][1]) < len(phase_data[i][1]) and d \
                    #             and phase_data[j][2] * phase_data[i][2] > 0 \
                    #             and abs(phase_data[j][2]) < abs(phase_data[i][2]):
                    #         del(phase_data_copy[j])
                    #         input()
                    #         n = 1
                    #     else:
                    #         # phase_data_copy.append(phase_data[j][1])
                    #         pass
            # else:
            #     phase_data_copy.append(phase_data[i][1])

        # print('融合结果2：', phase_data)
        function_desc = []
        phase_data_result = []
        for i in range(len(phase_data)):

            k = 0
            for j in range(len(phase_data)):
                if phase_data[i][0] != phase_data[j][0] and phase_data[i][1] == phase_data[j][1] \
                        and phase_data[i][2] == phase_data[j][2]:
                    if [phase_data[j][1], phase_data[j][2]] not in function_desc:
                        phase_data_comb = [[phase_data[i][0], phase_data[j][0]], phase_data[i][1],phase_data[i][2]]
                        phase_data_result.append(phase_data_comb)
                        function_desc.append([phase_data[j][1], phase_data[j][2]])
                    else:
                        pass
                    k = 1
                else:
                    pass
            if k == 0:
                phase_data_comb = [[phase_data[i][0]], phase_data[i][1], phase_data[i][2]]
                phase_data_result.append(phase_data_comb)
        # print('融合结果3：', phase_data_result)
        return phase_data_result
    else:
        pass
    return phase_setting


# 列表元素比较，去除重复项
def remove_list(a,b):
    for i in a:
        if i in b:
            b.remove(i)
    return b


# 列表元素比较，若a所有元素都在b中则返回1
def compare_list(a, b):
    sign = 1
    for i in a:
        if i in b:
            sign = sign and sign
        else:
            sign = 0
    if sign == 0:
        return 0
    else:
        return 1


def excess_list(a):
    b = []
    for i in a:
        if i not in b:
            b.append(i)
    return b


# 需求数值计算
def demand_value_cal(alarm_times, count_lane, length):
    if count_lane is not None and length > 0:
        demand_value = alarm_times*count_lane*1000/length
        # print('报警次数：', abs(alarm_times))
        # print('车道数：', count_lane)
        # print('路段长度：', length)
        # print('需求数值结果：', round(demand_value, 2))
    else:
        demand_value = 0
    return demand_value


def cn_to_eng(rdsect_desc):

    if rdsect_desc == '北进口':
        rdsect_desc_en = 'north entrance'
    elif rdsect_desc == '南进口':
        rdsect_desc_en = 'south entrance'
    elif rdsect_desc == '西进口':
        rdsect_desc_en = 'west entrance'
    elif rdsect_desc == '东进口':
        rdsect_desc_en = 'east entrance'
    elif rdsect_desc == '东出口':
        rdsect_desc_en = 'east exit'
    elif rdsect_desc == '南出口':
        rdsect_desc_en = 'south exit'
    elif rdsect_desc == '北出口':
        rdsect_desc_en = 'north exit'
    elif rdsect_desc == '西出口':
        rdsect_desc_en = 'west exit'
    else:
        rdsect_desc_en = rdsect_desc
    return rdsect_desc_en


def judge_demand(demand_value, function_desc):
    suggestion = []
    # if demand_value > 0:
    #     sug1 = "适当提高绿信比"
    function_num = len(function_desc)
    dir_desc = ""
    dir_desc_en = ""
    sug2 = ''
    sug2_en = ''
    for i in range(function_num):
        rdsect_desc = function_desc[i][0]
        rdsect_desc_en = cn_to_eng(rdsect_desc)
        function_id = function_desc[i][1]
        fun_desc = ""
        fun_desc_en = ""
        if function_id == '1':
            fun_desc = "直行"
            fun_desc_en = "straight"
        elif function_id == '2':
            fun_desc = "左转"
            fun_desc_en = "left"
        elif function_id == '3':
            fun_desc = "左转"
            fun_desc_en = "right"
        if i > 0:
            dir_desc = dir_desc+'、'+rdsect_desc
            dir_desc_en = dir_desc_en + ',' + rdsect_desc_en + " "
        else:
            dir_desc = dir_desc + rdsect_desc
            dir_desc_en = dir_desc_en + rdsect_desc_en + " "
    if demand_value > 0:
        sug2 = "建议适当提高绿信比，提高{0}方向通行能力".format(dir_desc)
        sug2_en = "Increase the Split to improve the capacity of {0}".format(dir_desc_en)
        # suggestion.append(sug2)
    if demand_value < 0:
        sug2 = "适当降低绿信比，降低{0}方向通行能力".format(dir_desc)
        sug2_en = "Decrease the Split to reduce the capacity of {0}".format(dir_desc_en)
        # suggestion.append(sug2)

    return sug2, sug2_en


# 绿信比建议
def suggest_translate(combine_data):
    green_ratio_num = len(combine_data)
    green_ratio_sug = []
    sug_phase = ""
    sug_phase_en = ""
    green_ratio_sug_en = []
    for i in range(len(combine_data)):
        phase_name = combine_data[i][0]
        function_desc = combine_data[i][1]
        demand_value = combine_data[i][2]
        if len(phase_name) == 1:
            sug_phase = "{0}相位：".format(phase_name[0])
            sug_phase_en = "Phase {0}：".format(phase_name[0])
        if len(phase_name) == 2:
            sug_phase = "{0}或{1}相位：".format(phase_name[0], phase_name[1])
            sug_phase_en = "Phase {0} or {1}：".format(phase_name[0], phase_name[1])
        if len(phase_name) == 3:
            sug_phase = "{0}、{1}或{2}相位：".format(phase_name[0], phase_name[1], phase_name[2])
            sug_phase_en = "Phase {0},{1}or {2}：".format(phase_name[0], phase_name[1], phase_name[2])
        sug_desc, sug_desc_en = judge_demand(demand_value, function_desc)
        suggestion_en = sug_phase_en+sug_desc_en
        suggestion = sug_phase+sug_desc
        # print(suggestion)
        green_ratio_sug.append(suggestion)
        green_ratio_sug_en.append(suggestion_en)
    if CONTENT.lang == 'cn':
        return green_ratio_sug
    elif CONTENT.lang == 'en':
        return green_ratio_sug_en


def call_oracle(start_time, end_time, start_date):
    match_plan_sl_info = pd.DataFrame({})
    sql = "select * from {0} where FSTR_DATE = '{1}'  and FSTR_CYCLE_STARTTIME between " \
          "'{2}' and '{3}' ORDER BY FSTR_INTERSECTID, FSTR_DATE, FSTR_CYCLE_STARTTIME" \
        .format(GlobalContent.dyna_plan_selection, start_date, start_time, end_time)
    try:
        db = cx_Oracle.connect(GlobalContent.OracleUser)
    except Exception as e:
        print('获取数据-连接oracle失败: ' + str(e))
        print(e)
    else:
        try:
            cr = db.cursor()
            cr.execute(sql)
            rows = cr.fetchall()
            match_plan_sl_info = pd.DataFrame(rows)
            # match_plan_sl_info.columns = ['FSTR_INTERSECTID', 'FSTR_DATE', 'FSTR_CYCLE_STARTTIME',
            #                               'FSTR_CYCLE_LENGTH', 'PHASE_A', 'PHASE_B', 'PHASE_C', 'PHASE_D',
            #                               'PHASE_E', 'PHASE_F', 'PHASE_G']
            cr.close()
        except Exception as e:
            print('获取数据-路口基础绿信比方案数据-失败: ' + str(e))
            print(e)
        else:
            if len(rows) > 0:
                match_plan_sl_info = pd.DataFrame(rows)
                match_plan_sl_info.columns = ['FSTR_INTERSECTID', 'FSTR_DATE', 'FSTR_CYCLE_STARTTIME',
                                              'FSTR_CYCLE_LENGTH', 'PHASE_A', 'PHASE_B', 'PHASE_C', 'PHASE_D',
                                              'PHASE_E', 'PHASE_F', 'PHASE_G']
                pass
        finally:
            db.close()

    return match_plan_sl_info


# 判断是否新增协调
def add_coor(grouped_dyna_plan_sl, up_scatsid, down_scatsid, rds_length):
    if_add_coor = False
    # start_time = str_time[:8]
    # end_time = str_time[-8:]
    # start_date = str_date[:10]
    # end_date = str_date[-10:]
    # match_plan_sl_info = call_oracle(start_time, end_time, start_date, end_date)
    # grouped_dyna_plan_sl = match_plan_sl_info.groupby(['FSTR_INTERSECTID'])
    # print(match_plan_sl_info)
    try:
        up_data = grouped_dyna_plan_sl.get_group(up_scatsid)
        down_data = grouped_dyna_plan_sl.get_group(down_scatsid)
        up_cycle = int(Counter(up_data['FSTR_CYCLE_LENGTH'].tolist()).most_common(1)[0][0])
        down_cycle = int(Counter(down_data['FSTR_CYCLE_LENGTH'].tolist()).most_common(1)[0][0])
        # print(str(up_cycle) + 'a' + str(down_cycle))
        cycle_dif = abs(up_cycle - down_cycle) + 1
        if cycle_dif > 50 or int(rds_length) > 500:
            if_add_coor = False
        else:
            if_add_coor = True
        # print(cycle_dif)
    except KeyError:
        pass
    # print(if_add_coor)
    return if_add_coor


def sug_creat(combine_data, alarm_times_grouped, node_id, phase_data, sign_coor, match_coor, coor_mark):
    empty = []
    green_suggest = []
    phase_suggest = []
    coor_suggest = []
    cycle_suggest = []
    other_sug = []
    all_suggestion = {}
    # print(combine_data)
    # input()
    if combine_data == 0:
        # print('路口拥堵状况较为复杂，请根据实际情况进行调整')
        if CONTENT.lang == 'cn':
            other_sug = '路口拥堵状况较为复杂，请根据实际情况进行调整'
        elif CONTENT.lang == 'en':
            other_sug = "The traffic congestion at the intersection is more complicated. Please adjust it according " \
                        "to the actual situation."

        all_suggestion = {'Split': green_suggest, 'Cycle': cycle_suggest, 'Phase': phase_suggest,
                          'Coordination': coor_suggest,
                          'Other': other_sug}
    elif len(combine_data) > 0:
        green_suggest = suggest_translate(combine_data)
        phase_suggest = phase_suggests(combine_data, alarm_times_grouped, node_id)
        cycle_suggest, coor_suggest = coor_suggest_create(phase_data, combine_data, node_id, sign_coor, match_coor, coor_mark)
        all_suggestion = {'Split': green_suggest, 'Cycle': cycle_suggest, 'Phase': phase_suggest,
                          'Coordination': coor_suggest,
                          'Other': other_sug}

    elif len(combine_data) == 0:
        all_suggestion = {'Split': green_suggest, 'Cycle': cycle_suggest, 'Phase': phase_suggest,
                          'Coordination': coor_suggest,
                          'Other': other_sug}
    if len(cycle_suggest) > 0:

        # print(coor_suggest)
        # print(cycle_suggest)
        # print(phase_data)

        pass
    # if len(green_suggest) + len(cycle_suggest) + len(phase_suggest) + len(coor_suggest) + len(other_sug) == 0:
    #     return empty
    # else:
    return all_suggestion
    # print('finall=======================================================================')
    # print(all_suggestion)


# 相位建议
def phase_suggests(combine_data, alarm_times_grouped, node_id):
    import_desc = ['东进口', '南进口', '西进口', '北进口']
    oppsite_desc = ['西进口', '北进口', '东进口', '南进口']
    dir_desc = ""
    dir_desc_en =""
    for i in range(len(combine_data)):
        phase_name = combine_data[i][0]
        function_desc = combine_data[i][1]
        demand_value = combine_data[i][2]
        if len(phase_name) == 1 and len(function_desc) == 1 and demand_value > 0:
            function_num = len(function_desc)
            dir_desc = ""
            dir_desc_en = ""
            sug = []
            sug_en = []
            for i in range(function_num):
                rdsect_desc = function_desc[i][0]
                rdsect_desc_en = cn_to_eng(rdsect_desc)
                index = import_desc.index(function_desc[i][0])
                opp_alarm_times = 0
                try:
                    opp_desc = oppsite_desc[index]
                    opp_alarm_times = alarm_times_grouped[node_id][opp_desc]
                except KeyError:
                    # print("对向进口道无报警")
                    pass
                if opp_alarm_times == 0:
                    function_id = function_desc[i][1]

                if i > 0:
                    dir_desc = dir_desc + '、' + rdsect_desc
                    dir_desc_en = dir_desc_en + '、' + rdsect_desc_en
                else:
                    dir_desc = dir_desc + rdsect_desc
                    dir_desc_en = dir_desc_en + rdsect_desc_en
    if len(dir_desc) > 1:
        sug = ["如果{0}未设置同放相位，建议新增同放相位".format(dir_desc)]
        sug_en = ["If {0} does not set simultaneous release phase,it is recommended to add "
                  "simultaneous release phase".format(dir_desc_en)]
    else:
        sug = []
        sug_en = []
    if CONTENT.lang == 'cn':
        return sug
    elif CONTENT.lang == 'en':
        return sug_en


# 生成所有车要车流组合
def main_flow(conn, alarm_data, node_id, alarm_times_grouped, coor_match_rdsect,  new_phase_match_data,
              new_lane_inf, sign_coor, coor_mark, phase_dir):
    flow_combine = GlobalContent.flow_combine
    all_combine = []
    phase_data = []
    match_coor = 0
    # print(alarm_data)
    # alarm_data-[['48', '东进口', 'UTRSS000686', 24]]
    # 出口道的路段ID属于另一个路口
    # 匹配协调方案相位功能
    for i in range(len(coor_match_rdsect)):
        phase_fun = []
        # 节点编号，相位，路段编号一致
        # 进口道
        # print('++++++++++++++++++++++++++++++++++++++++++')
        # print(coor_match_rdsect)
        # print(new_phase_match_data)
        # print('++++++++++++++++++++++++++++++++++++++++++')
        for j in range(len(new_phase_match_data)):
            if coor_match_rdsect[i][1] == new_phase_match_data[j][0] and coor_match_rdsect[i][3] == new_phase_match_data[j][2]:
                    # and coor_match_rdsect[i][2] == new_phase_match_data[4]:
                phase_fun.append(new_phase_match_data[j][3])
                # print("phase_fun:", phase_fun)
                phase_data.append([coor_match_rdsect[i][2], coor_match_rdsect[i][3], phase_fun, 1, coor_match_rdsect[i][4]])
            # 出口道
            if coor_match_rdsect[i][0] == new_phase_match_data[j][0] and coor_match_rdsect[i][3] == new_phase_match_data[j][2]:
                phase_fun.append(new_phase_match_data[j][3])
                # print("phase_fun:", phase_fun)
                # print("coor_match_rdsect:",coor_match_rdsect[i])
                phase_data.append([coor_match_rdsect[i][2], coor_match_rdsect[i][3], phase_fun, 0, coor_match_rdsect[i][4]]) # 相位名字+功能

    # 相位功能拆分
    if len(phase_data)>0:
        # print("phase_data:", phase_data)
        phase_data = excess_list(phase_data)
        # print("phase_data:", phase_data)
        for j in range(len(phase_data)):
            flow_dir = phase_data[j][2]
            split_fun = []
            for m in flow_dir:
                fun = m.split(';')
                for n in fun:
                    if n not in split_fun:
                        split_fun.append(n)
                # print(split_fun)
            phase_data[j][2] = split_fun
            # print('phase_data:', phase_data)   # [['UTRSS001114', 'D', ['1', '3', '2'], 0, '南出口']]
    # 生成所有流向选项
    for m in range(len(alarm_data)):
        combine = []
        for n in range(len(flow_combine)):
            flow = flow_combine[n]
            combine.append([alarm_data[m], flow])
        all_combine.append(combine)

        for n in range(len(phase_data)):
            # print(phase_data[n][0], alarm_data[m][2])
            if phase_data[n][0] == alarm_data[m][2]:
                match_coor += 1
    # print(all_combine)
    if len(all_combine) == 1:
        choose_event = [all_combine[0][1][0][1]]
        choose_event_en = [cn_to_eng(all_combine[0][1][0][1])]
        # print('选项：', choose_event)
        all_result = []
        order_event = []
        order_event_show = []
        for i in choose_event:
            order = []
            order_en = []
            order2 = phase_dir[i]
            order_show = []
            order_en_show = []
            if i == '东出口':
                order = ['西', '北', '南']
                order_en = ['west', 'north', 'south']
            if i == '东进口':
                order = ['西', '南', '北']
                order_en = ['west', 'south', 'north']
            if i == '北出口':
                order = ['南', '西', '东']
                order_en = ['south', 'west', 'east']
            if i == '北进口':
                order = ['南', '东', '西']
                order_en = ['south', 'east', 'west']
            if i == '西出口':
                order = ['东', '南', '北']
                order_en = ['east', 'south', 'north']
            if i == '西进口':
                order = ['东', '北', '南']
                order_en = ['east', 'north', 'south']
            if i == '南出口':
                order = ['北', '东', '西']
                order_en = ['north', 'east', 'west']
            if i == '南进口':
                order = ['北', '西', '东']
                order_en = ['north', 'west', 'east']
            for j in range(len(order)):
                if order[j] in order2:
                    order_show.append(order[j])
                    order_en_show.append(order_en[j])
            if CONTENT.lang == 'cn':
                order_event.append(order)
                order_event_show.append(order_show)
            elif CONTENT.lang == 'en':
                order_event.append(order_en)
                order_event_show.append(order_en_show)
        # 匹配协调方案
        for i in range(3):
            key = order_event[0][i]
            input_data = [all_combine[0][i]]
            phase_match = flow_match(input_data, new_phase_match_data, new_lane_inf)
            combine_data = phase_combine(phase_match)

            # print("phase_match:", phase_match)
            # print(combine_data)
            all_suggestion = sug_creat(combine_data, alarm_times_grouped, node_id, phase_data, sign_coor, match_coor, coor_mark)
            # print('finall=======================================================================')
            # print(all_suggestion)
            # input()
            result = {'Key': key, 'Sug': all_suggestion}
            if len(all_suggestion) > 0:
                all_result.append(result)
        if CONTENT.lang == 'cn':
            finall_result = {'ChooseEvent': choose_event, 'ChoseOrder': order_event_show,'AllSug': all_result}
        elif CONTENT.lang == 'en':
            finall_result = {'ChooseEvent': choose_event_en, 'ChoseOrder': order_event_show, 'AllSug': all_result}
        else:
            finall_result = {}
        # print(finall_result)
        json_result = json.dumps(finall_result, ensure_ascii=False)
        return json_result

    elif len(all_combine) == 2:
        num = 3 ** 2
        all_result = []
        choose_event = [all_combine[0][1][0][1], all_combine[1][1][0][1]]
        choose_event_en = [cn_to_eng(all_combine[0][1][0][1]), cn_to_eng(all_combine[1][1][0][1])]
        order_event = []
        order_event_show = []
        for i in choose_event:
            order = []
            order_en = []
            order2 = phase_dir[i]
            order_show = []
            order_en_show = []

            if i == '东出口':
                order = ['西', '北', '南']
                order_en = ['west', 'north', 'south']
            if i == '东进口':
                order = ['西', '南', '北']
                order_en = ['west', 'south', 'north']
            if i == '北出口':
                order = ['南', '西', '东']
                order_en = ['south', 'west', 'east']
            if i == '北进口':
                order = ['南', '东', '西']
                order_en = ['south', 'east', 'west']
            if i == '西出口':
                order = ['东', '南', '北']
                order_en = ['east', 'south', 'north']
            if i == '西进口':
                order = ['东', '北', '南']
                order_en = ['east', 'north', 'south']
            if i == '南出口':
                order = ['北', '东', '西']
                order_en = ['north', 'east', 'west']
            if i == '南进口':
                order = ['北', '西', '东']
                order_en = ['north', 'west', 'east']

            for j in range(len(order)):
                if order[j] in order2:
                    order_show.append(order[j])
                    order_en_show.append(order_en[j])
            if CONTENT.lang == 'cn':
                order_event.append(order)
                order_event_show.append(order_show)
            elif CONTENT.lang == 'en':
                order_event.append(order_en)
                order_event_show.append(order_en_show)
        # print('选项：', choose_event)
        for i in range(3):
            for j in range(3):
                # key = [i, j]
                key = [order_event[0][i], order_event[1][j]]
                input_data = [all_combine[0][i], all_combine[1][j]]
                phase_match = flow_match(input_data, new_phase_match_data, new_lane_inf)
                combine_data = phase_combine(phase_match)
                # print(len(combine_data))

                all_suggestion = sug_creat(combine_data, alarm_times_grouped, node_id, phase_data, sign_coor, match_coor, coor_mark)
                # print('finall=======================================================================')
                # print(all_suggestion)
                result = {'Key': key, 'Sug': all_suggestion}
                all_result.append(result)

        if CONTENT.lang == 'cn':
            finall_result = {'ChooseEvent': choose_event, 'ChoseOrder': order_event_show,'AllSug': all_result}
        elif CONTENT.lang == 'en':
            finall_result = {'ChooseEvent': choose_event_en, 'ChoseOrder': order_event_show, 'AllSug': all_result}
        else:
            finall_result = {}
        # print(finall_result)
        json_result = json.dumps(finall_result, ensure_ascii=False)
        return json_result

    elif len(all_combine) == 3:
        num = 3 ** 3
        all_result = []
        choose_event = [all_combine[0][1][0][1], all_combine[1][1][0][1], all_combine[2][1][0][1]]
        choose_event_en = [cn_to_eng(all_combine[0][1][0][1]), cn_to_eng(all_combine[1][1][0][1]),
                           cn_to_eng(all_combine[2][1][0][1])]
        # print('选项：', choose_event)
        order_event = []
        order_event_show = []
        for i in choose_event:
            order = []
            order_en = []
            order2 = phase_dir[i]
            order_show = []
            order_en_show = []

            if i == '东出口':
                order = ['西', '北', '南']
                order_en = ['west', 'north', 'south']
            if i == '东进口':
                order = ['西', '南', '北']
                order_en = ['west', 'south', 'north']
            if i == '北出口':
                order = ['南', '西', '东']
                order_en = ['south', 'west', 'east']
            if i == '北进口':
                order = ['南', '东', '西']
                order_en = ['south', 'east', 'west']
            if i == '西出口':
                order = ['东', '南', '北']
                order_en = ['east', 'south', 'north']
            if i == '西进口':
                order = ['东', '北', '南']
                order_en = ['east', 'north', 'south']
            if i == '南出口':
                order = ['北', '东', '西']
                order_en = ['north', 'east', 'west']
            if i == '南进口':
                order = ['北', '西', '东']
                order_en = ['north', 'west', 'east']

            for j in range(len(order)):
                if order[j] in order2:
                    order_show.append(order[j])
                    order_en_show.append(order_en[j])
            if CONTENT.lang == 'cn':
                order_event.append(order)
                order_event_show.append(order_show)
            elif CONTENT.lang == 'en':
                order_event.append(order_en)
                order_event_show.append(order_en_show)

        for i in range(3):
            for j in range(3):
                for k in range(3):
                    # key = [i, j, k]
                    key = [order_event[0][i], order_event[1][j], order_event[2][k]]
                    input_data = [all_combine[0][i], all_combine[1][j], all_combine[2][k]]
                    phase_match = flow_match(input_data, new_phase_match_data, new_lane_inf)
                    combine_data = phase_combine(phase_match)
                    # print(len(combine_data))

                    all_suggestion = sug_creat(combine_data, alarm_times_grouped, node_id, phase_data, sign_coor, match_coor, coor_mark)
                    # print('finall=======================================================================')
                    # print(all_suggestion)
                    result = {'Key': key, 'Sug': all_suggestion}
                    all_result.append(result)

        if CONTENT.lang == 'cn':
            finall_result = {'ChooseEvent': choose_event, 'ChoseOrder': order_event,'AllSug': all_result}
        elif CONTENT.lang == 'en':
            finall_result = {'ChooseEvent': choose_event_en, 'ChoseOrder': order_event, 'AllSug': all_result}
        else:
            finall_result = {}
        # print(finall_result)
        json_result = json.dumps(finall_result, ensure_ascii=False)
        return json_result

    elif len(all_combine) == 4:
        choose_event = [all_combine[0][1][1], all_combine[1][1][0][1], all_combine[2][1][0][1], all_combine[3][1][0][1]]
        choose_event_en = [cn_to_eng(all_combine[0][1][0][1]), cn_to_eng(all_combine[1][1][0][1]),
                           cn_to_eng(all_combine[2][1][0][1]), cn_to_eng(all_combine[3][1][0][1])]
        # print('选项：', choose_event)
        all_result = []
        order_event = []

        order_event_show = []
        for i in choose_event:
            order = []
            order_en = []
            order2 = phase_dir[i]
            order_show = []
            order_en_show = []

            if i == '东出口':
                order = ['西', '北', '南']
                order_en = ['west', 'north', 'south']
            if i == '东进口':
                order = ['西', '南', '北']
                order_en = ['west', 'south', 'north']
            if i == '北出口':
                order = ['南', '西', '东']
                order_en = ['south', 'west', 'east']
            if i == '北进口':
                order = ['南', '东', '西']
                order_en = ['south', 'east', 'west']
            if i == '西出口':
                order = ['东', '南', '北']
                order_en = ['east', 'south', 'north']
            if i == '西进口':
                order = ['东', '北', '南']
                order_en = ['east', 'north', 'south']
            if i == '南出口':
                order = ['北', '东', '西']
                order_en = ['north', 'east', 'west']
            if i == '南进口':
                order = ['北', '西', '东']
                order_en = ['north', 'west', 'east']

            for j in range(len(order)):
                if order[j] in order2:
                    order_show.append(order[j])
                    order_en_show.append(order_en[j])
            if CONTENT.lang == 'cn':
                order_event.append(order)
                order_event_show.append(order_show)
            elif CONTENT.lang == 'en':
                order_event.append(order_en)
                order_event_show.append(order_en_show)

        for i in range(3):
            for j in range(3):
                for k in range(3):
                    for l in range(3):
                        # key = [i, j, k, l]
                        key = [order_event[0][i], order_event[1][j], order_event[2][k], order_event[3][l]]
                        input_data = [all_combine[0][i], all_combine[1][j], all_combine[2][k], all_combine[3][l]]
                        phase_match = flow_match(input_data, new_phase_match_data, new_lane_inf)
                        combine_data = phase_combine(phase_match)
                        # print(len(combine_data))

                        all_suggestion = sug_creat(combine_data, alarm_times_grouped, node_id, phase_data, sign_coor, match_coor, coor_mark)
                        # print('finall=======================================================================')
                        result = {'Key': key, 'Sug': all_suggestion}
                        all_result.append(result)
        if CONTENT.lang == 'cn':
            finall_result = {'ChooseEvent': choose_event, 'ChoseOrder': order_event_show,'AllSug': all_result}
        elif CONTENT.lang == 'en':
            finall_result = {'ChooseEvent': choose_event_en, 'ChoseOrder': order_event_show, 'AllSug': all_result}
        else:
            finall_result = {}
        # print(finall_result)
        json_result = json.dumps(finall_result, ensure_ascii=False)
        return json_result

    elif len(all_combine) >= 5:
        choose_event = []
        choose_event_en = []
        order_event = []
        all_result = []
        combine_data = 0
        green_suggest = []
        phase_suggest = []
        coor_suggest = []
        cycle_suggest = []
        other_sug = []
        all_suggestion = {}
        if combine_data == 0:
            key = []
            # print('路口拥堵状况较为复杂，请根据实际情况进行调整')
            other_sug = '路口拥堵状况较为复杂，请根据实际情况进行调整'
            all_suggestion = {'Split': green_suggest,'Cycle': cycle_suggest, 'Phase': phase_suggest,
                              'Coordination': coor_suggest,
                               'Other': other_sug}
            result = {'Key': key, 'Sug': all_suggestion}
            all_result.append(result)
        if CONTENT.lang == 'cn':
            finall_result = {'ChooseEvent': choose_event, 'ChoseOrder': order_event,'AllSug': all_result}
        elif CONTENT.lang == 'en':
            finall_result = {'ChooseEvent': choose_event_en, 'ChoseOrder': order_event, 'AllSug': all_result}
        else:
            finall_result = {}
        # print(finall_result)
        json_result = json.dumps(finall_result, ensure_ascii=False)
        return json_result
        # input_data = [all_combine[0][i], all_combine[1][j], all_combine[2][k], all_combine[3][l],
        #               all_combine[4][m]]
        # phase_match = flow_match(conn, input_data)
        # combine_data = phase_combine(phase_match)

    # elif len(all_combine) == 6:
    #     for i in range(3):
    #         for j in range(3):
    #             for k in range(3):
    #                 for l in range(3):
    #                     for m in range(3):
    #                         for n in range(3):
    #                             combine_array = [i, j, k, l, m, n]
    #                             input_data = [all_combine[0][i], all_combine[1][j], all_combine[2][k], all_combine[3][l],
    #                                           all_combine[4][m], all_combine[5][n]]
    #                             phase_match = flow_match(conn, input_data)
    #                             combine_data = phase_combine(phase_match)
    #
    # elif len(all_combine) == 7:
    #     for i in range(3):
    #         for j in range(3):
    #             for k in range(3):
    #                 for l in range(3):
    #                     for m in range(3):
    #                         for n in range(3):
    #                             for o in range(3):
    #                                 combine_array = [i, j, k, l, m, n, o]
    #                                 input_data = [all_combine[0][i], all_combine[1][j], all_combine[2][k], all_combine[3][l],
    #                                               all_combine[4][m], all_combine[5][n], all_combine[6][o]]
    #                                 phase_match = flow_match(conn, input_data)
    #                                 combine_data = phase_combine(phase_match)
    # elif len(all_combine) == 8:
    #     for i in range(3):
    #         for j in range(3):
    #             for k in range(3):
    #                 for l in range(3):
    #                     for m in range(3):
    #                         for n in range(3):
    #                             for o in range(3):
    #                                 for p in range(3):
    #                                     combine_array = [i, j, k, l, m, n, o, p]
    #                                     input_data = [all_combine[0][i], all_combine[1][j], all_combine[2][k],
    #                                                   all_combine[3][l],
    #                                                   all_combine[4][m], all_combine[5][n], all_combine[6][o],
    #                                                   all_combine[7][p]]
    #                                     phase_match = flow_match(conn, input_data)
    #                                     combine_data = phase_combine(phase_match)
    else:
        print('拥堵路段超出预计数量')
    pass


# 相位信息合并
def phase_inf_combine(scats_id, original_data):

    match_data = []
    for i in range(len(original_data)):
        phase_nmae = original_data[i][2]
        phase_function = original_data[i][3]
        rdsectid = original_data[i][4]
        veh_desc = original_data[i][5]
        flow_dir = original_data[i][6]
        # print(scats_id)
        # print(original_data)
        flow_dir = flow_dir.split(';')
        phase_function = phase_function.split(';')
        # print(veh_desc, phase_function)
        match_data.append([scats_id, phase_nmae, phase_function, veh_desc, flow_dir, rdsectid])
    # print(match_data)
    return match_data


def list_to_str(a, split_str):
    desc = ''
    for i in range(len(a)):
        if i == 0:
            desc = a[i]
        else:
            desc = desc + split_str + a[i]
    return desc


# 协调方向建议及周期建议
def coor_suggest_create(phase_data, combine_data, match_node, sign_coor, match_coor, coor_mark):
    # combine_data - [[['A'], [['西进口', '1']], Decimal('-342.39')]] 只有进口道
    # phase_data - [['UTRSS001114', 'D', ['1', '3', '2'], 0, '南出口']] 协调方案

    phase_sug = []
    phase_setting_po = []
    phase_setting_ne = []
    cycle_sug = []
    cycle_sug_en = []
    coor_sugs = []      # 协调建议集合
    coor_sugs_en = []
    dir_demand = []     # 需求数据集合
    dir_desc_setting = []   # 进出口道描述的集合
    phase_dir_desc = []
    phase_dir_desc_back = []
    for i in range(len(phase_data)):
        phase_dir_desc.append(phase_data[i][4])
        if phase_data[i][4][1:] == "进口":
            phase_dir_back = phase_data[i][4][0]+"出口"
            phase_dir_desc_back.append(phase_dir_back)
        elif phase_data[i][4][1:] == "出口":
            phase_dir_back = phase_data[i][4][0] + "进口"
            phase_dir_desc_back.append(phase_dir_back)

    positive_num = 0
    negitive_num = 0
    for i in range(len(combine_data)):
        if combine_data[i][2] > 0:
            # 需求为正的相位集合
            phase_setting_po.append(combine_data[i][0])
        elif combine_data[i][2] < 0:
            # 需求为负的相位集合
            phase_setting_ne.append(combine_data[i][0])
        dir_demand.append(combine_data[i][2])

    sum_demand = sum(dir_demand)
    for i in range(len(dir_demand)):
        if dir_demand[i] > 0:
            positive_num += 1
        if dir_demand[i] < 0:
            negitive_num += 1

    for i in range(len(combine_data)):
        dir_desc_com = []
        dir_desc_com_back = []
        dir_desc = combine_data[i][1]
        for j in dir_desc:
            dir_desc_setting.append(j[0])
            dir_desc_com.append(j[0])
            dir_desc_com_back.append(j[0][0]+"出口")
            # print(dir_desc_com)
            # print(dir_desc_com_back)
            # print('_______________________________________________')
        for j in range(len(phase_data)):
            coor_sug = ""
            coor_sug_en = ""
            # 路段相位相同
            # print(phase_data[j])
            # print(combine_data[i])
            # print('_______________________________________________')
            if phase_data[j][1] in combine_data[i][0]:
                # 匹配同向协调
                # print(phase_data[j][4])
                # print(dir_desc_com)
                # print('_______________________________________________')
                if phase_data[j][4] in dir_desc_com:
                        # 提高cap
                        dir_desc_en = cn_to_eng(phase_data[j][4])
                        if combine_data[i][2] > 0:
                            coor_sug = "建议适当降低{0}与{1}相位关联的相位差，增加{2}清空时间" \
                                .format(phase_data[j][4], phase_data[j][1], phase_data[j][4])
                            coor_sug_en = "Reduce the phase offset associated " \
                                          "with {0} {1} phase to increase the {2} clear time."\
                                .format(dir_desc_en, phase_data[j][1], dir_desc_en)
                        if combine_data[i][2] < 0:
                            coor_sug = "建议适当提高{0}与{1}相位关联的相位差，降低{2}清空时间" \
                                .format(phase_data[j][4], phase_data[j][1], phase_data[j][4])
                            coor_sug_en = "Increase the phase offset associated " \
                                          "with {0} {1} phase to increase the {2} clear time." \
                                .format(dir_desc_en, phase_data[j][1], dir_desc_en)
                        if len(coor_sug) > 0:
                            if CONTENT.lang == 'cn':
                                coor_sugs.append(coor_sug)
                            elif CONTENT.lang == 'en':
                                coor_sugs.append(coor_sug_en)
                # 反向路段有协调
                elif phase_data[j][4] in dir_desc_com_back:
                    dir_desc_back = phase_data[j][4][0]+"出口"
                    dir_desc_forward = phase_data[j][4][0] + "进口"
                    dir_desc_back_en = cn_to_eng(dir_desc_back)
                    dir_desc_forward_en = cn_to_eng(dir_desc_forward)
                    if combine_data[i][2] > 0:

                        coor_sug = "建议适当提高{0}与{1}相位关联的相位差，增加{2}清空时间" \
                            .format(dir_desc_back, phase_data[j][1], dir_desc_forward)
                        coor_sug_en = "Increase the phase offset associated " \
                                      "with {0} {1} phase to increase the {2} clear time." \
                            .format(dir_desc_back_en, phase_data[j][1], dir_desc_forward_en)
                    if combine_data[i][2] < 0:
                        coor_sug = "建议适当降低{0}与{1}相位关联的相位差，降低{2}清空时间" \
                            .format(dir_desc_back, phase_data[j][1], dir_desc_forward)
                        coor_sug_en = "Reduce the phase offset associated " \
                                      "with {0} {1} phase to increase the {2} clear time." \
                            .format(dir_desc_back_en, phase_data[j][1], dir_desc_forward_en)

                    if len(coor_sug) > 0:
                            coor_sugs.append(coor_sug)
                            coor_sugs_en.append(coor_sug_en)

        phase_com = ""
        if len(combine_data[i][0]) >= 1:
            phase_sec = []
            phase_str = ''
            phase_main = ''
            for j in range(len(combine_data[i][0])):
                if j == 0:
                    phase_main = combine_data[i][0][j]
                else:
                    phase_sec.append(combine_data[i][0][j])
            phase_sec = excess_list(phase_sec)
            phase_str = list_to_str(phase_sec, ',')
            # print(phase_str)
            if len(phase_str) > 0:
                phase_com = phase_main + '(' + phase_str + ')'
            else:
                phase_com = phase_main

        for j in range(len(coor_mark)):
            coor_sug = ""
            coor_sug_en = ""

            if coor_mark[j][0] in dir_desc_com and coor_mark[j][0] not in phase_dir_desc and \
                    coor_mark[j][0] not in phase_dir_desc_back and coor_mark[j][2] == True:
                if combine_data[i][2] > 0 and abs(positive_num-negitive_num) < 2:
                    dir_desc_en = cn_to_eng(coor_mark[j][0])
                    coor_sug = "建议新增{0}与{1}相位关联的协调方案，以增加{2}清空时间" \
                        .format(coor_mark[j][0], phase_com, coor_mark[j][0])
                    coor_sug_en = "Design coordination plan related to {0} " \
                                  "phase {1} to increase the {2} clearing time"\
                        .format(dir_desc_en, phase_com, dir_desc_en)

                if combine_data[i][2] < 0 and abs(positive_num-negitive_num) < 2:
                    dir_desc_en = cn_to_eng(coor_mark[j][0])
                    coor_sug = "建议新增{0}与{1}相位关联的协调方案，以降低{2}清空时间" \
                        .format(coor_mark[j][0], phase_com, coor_mark[j][0])
                    coor_sug_en = "Design coordination plan related to {0} " \
                                  "phase {1} to reduce the {2} clearing time"\
                        .format(dir_desc_en, phase_com, dir_desc_en)
                if len(coor_sug) > 0:
                        coor_sugs.append(coor_sug)
                        coor_sugs_en.append(coor_sug_en)

    # print(sum_demand, positive_num-negitive_num, sign_coor, match_coor)

    if sum_demand > 0 and positive_num-negitive_num >= 2:
        phase_desc = ""
        # print(phase_setting_po)
        for i in range(len(phase_setting_po)):
            phase_com = ''
            if len(phase_setting_po[i]) >= 1:
                phase_sec = []
                phase_str = ''
                phase_main = ''
                for j in range(len(phase_setting_po[i])):
                    if j == 0:
                        phase_main = phase_setting_po[i][j]
                    else:
                        phase_sec.append(phase_setting_po[i][j])
                phase_sec = excess_list(phase_sec)
                phase_str = list_to_str(phase_sec, ',')
                # print(phase_str)

                if len(phase_str) > 0:
                    phase_com = phase_main + '(' + phase_str + ')'
                else:
                    phase_com = phase_main
            if i == 0:
                phase_desc = phase_desc + phase_com
            elif i >= 1:
                phase_desc = phase_desc + ',' + phase_com

        if sign_coor == 0:
            cycle_sug = ["建议适当增大路口周期，增加{0}相位时长".format(phase_desc)]
            cycle_sug_en = ["Increase the intersection period appropriately and increase "
                            "the phase length of {0}".format(phase_desc)]
        elif sign_coor == match_coor:
            cycle_sug = ["建议断开协调方案并增大路口周期，增加{0}相位时长".format(phase_desc)]
            cycle_sug_en = ["Break the coordination plan and increase "
                            "the intersection period, increasing the phase length of {0}".format(phase_desc)]
        else:
            cycle_sug = []
            cycle_sug_en = []
    elif sum_demand < 0 and negitive_num-positive_num >= 2:
        phase_desc = ""
        for i in range(len(phase_setting_ne)):
            if len(phase_setting_ne[i]) >= 1:
                phase_sec = []
                phase_str = ''
                phase_main = ''
                phase_com = ''
                for j in range(len(phase_setting_ne[i])):
                    if j == 0:
                        phase_main = phase_setting_ne[i][j]
                    else:
                        phase_sec.append(phase_setting_ne[i][j])
                phase_sec = excess_list(phase_sec)
                phase_str = list_to_str(phase_sec, ',')
                # print(phase_str)

                if len(phase_str) > 0:
                    phase_com = phase_main + '(' + phase_str + ')'
                else:
                    phase_com = phase_main
            if i == 0:
                phase_desc = phase_desc + phase_com
            elif i >= 1:
                phase_desc = phase_desc + ',' + phase_com
        if sign_coor == 0:
            cycle_sug = ["建议适当降低路口周期，减小{0}相位时长".format(phase_desc)]
            cycle_sug_en = ["It is recommended to reduce the intersection period appropriately and reduce "
                            "the phase length of {0}".format(phase_desc)]
        elif sign_coor == match_coor:
            cycle_sug = ["建议断开协调方案并降低路口周期，以减小{0}相位时长".format(phase_desc)]
            cycle_sug_en = ["It is recommended to break the plan and reduce the "
                            "intersection period to reduce the duration of phase {0} ".format(phase_desc)]
        else:
            cycle_sug = []
            cycle_sug_en = []
        # print(positive_num, negitive_num)
        # input()
    else:
        pass

    if CONTENT.lang == 'cn':
        return cycle_sug, coor_sugs
    elif CONTENT.lang == 'en':
        return cycle_sug_en, coor_sugs_en


def json_send(start_date, start_time, end_time, conn, node_id, json_data):
    start_date = str(start_date.replace('-', ''))
    end_date = str(start_date.replace('-', ''))
    cr = conn.cursor()
    data_type = 3
    sql = "insert into {0} values(%s,%s,%s,%s,%s,%s,%s)".format(GlobalContent.alarm_transfer_json_data)
    sql_delete = "delete from {0} where data_type = '{1}' and sub_id = '{2}' and start_date = '{3}' " \
                 "and end_date = '{4}' and start_time = '{5}' and end_time = '{6}'"\
        .format(GlobalContent.alarm_transfer_json_data, data_type, node_id, start_date, end_date,  start_time, end_time)
    # print(node_id,json_data)
    try:
        cr.execute(sql, (data_type, node_id, start_date, end_date,  start_time, end_time, json_data))
    except psycopg2.IntegrityError:
        conn.commit()
        cr.execute(sql_delete)
        cr.execute(sql, (data_type, node_id, start_date, end_date, start_time, end_time, json_data))
        conn.commit()
        print('数据重复,已删除原数据并重新插入')
    conn.commit()


def judge_node_coor(coor_data_node,scats_id):
    pass

def phase_dir_com(phase_match_data):
    entrance_set = []
    exit_set = []
    phase_dir = {}
    for i in range(len(phase_match_data[5])):
        entrance = phase_match_data[i][5]
        exit = phase_match_data[i][6]
        exit_split = exit.split(';')
        entrance_set.append(entrance)
        exit_set.append(exit_split)
    for i in range(len(entrance_set)):
        key = phase_dir.keys()
        if entrance_set[i] not in key:
            phase_dir[entrance_set[i]] = []
            for j in exit_set[i]:
                phase_dir[entrance_set[i]].append(j[0])
        elif entrance_set[i] in key:
            for j in exit_set[i]:
                if j[0] not in phase_dir[entrance_set[i]]:
                    phase_dir[entrance_set[i]].append(j[0])
                else:
                    pass
        for j in exit_set[i]:
            if j not in key:
                phase_dir[j] = []
                phase_dir[j].append(entrance_set[i][0])
            elif j in key:
                if entrance_set[i][0] not in phase_dir[j]:
                    phase_dir[j].append(entrance_set[i][0])
                else:
                    pass
    return phase_dir


# main
def main(start_date, start_time, end_time, isworkday):
    try:
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
    except Exception as e:
        print('database conn error', e)
    else:
        # 初始化及数据获取
        sign_coor = 0
        group_id_list = []
        up_node = []
        alarm_rdsect = call_alarm_rdsect(conn)
        # 获取各路段相位及功能信息
        node_inf = call_node_inf()
        match_plan_sl_info = call_oracle(start_time, end_time, start_date)
        if len(match_plan_sl_info) > 0:
            grouped_dyna_plan_sl = match_plan_sl_info.groupby(['FSTR_INTERSECTID'])
        else:
            grouped_dyna_plan_sl = pd.DataFrame({})

        alarm_times = call_alarm_times(conn,start_date, start_time, end_time)
        group_data_node = call_group_node(conn, start_date, start_time, end_time)
        group_data_road = call_group_rdsect(conn,start_date, start_time, end_time)
        coor_data_node = call_coor_dir(conn, start_time, end_time, node_inf)
        phase_match_data = call_phase_match(conn)
        lane_inf = call_lane_inf(conn)


        # print(lane_inf)
        # print(coor_data_node)
        if len(alarm_times)>0:
            if isworkday == 1 :
                alarm_times_grouped = alarm_times_match(alarm_times)
            elif isworkday == 0:
                alarm_times_grouped = alarm_times_match(alarm_times)
            elif isworkday == 2:
                alarm_times_grouped = alarm_times_match(alarm_times)

        if len(group_data_road) > 0:
            group_id_list = np.array(group_data_road['group_id']).tolist()
            group_id_list = set(group_id_list)
            group_id_list = sorted(group_id_list)
        for k in group_id_list:
            match_data = group_data_road[group_data_road['group_id'] == k]
            match_node = []
            for j in group_data_node:
                if j[0] == k:
                    match_node.append(j)
            # 路段结构组团信息
            coor_up_node = np.array(coor_data_node['up_node']).tolist()
            coor_down_node = np.array(coor_data_node['down_node']).tolist()
            coor_rdsectid = np.array(coor_data_node['rdsectid']).tolist()
            coor_up_phase = np.array(coor_data_node['up_phase']).tolist()
            coor_down_phase = np.array(coor_data_node['down_phase']).tolist()
            up_node = np.array(match_data['up_node']).tolist()
            down_node = np.array(match_data['down_node']).tolist()
            rdsectid = np.array(match_data['rdsectid']).tolist()


            # print(match_node)
            # print(up_node)
            # print(down_node)
            # 输出各节点的进出口路段
            for i in range(len(match_node)):
                alarm_data = []
                coor_up_match = []
                coor_down_match = []
                coor_up_phase_rdsect = []
                coor_down_phase_rdsect = []
                coor_export_rdsect = []
                coor_import_rdsect = []
                export_rdsect = []
                import_rdsect = []
                coor_cycle = 0
                # 匹配车道及相位信息
                new_lane_inf = [x for x in lane_inf if x[0] == match_node[i][1]]
                new_phase_match_data = [x for x in phase_match_data if x[0] == match_node[i][1]]
                # print(new_phase_match_data)
                phase_dir = phase_dir_com(phase_match_data)
                coor_mark = []
                for j in range(len(up_node)):
                    # 属于该节点的路段，出口为出口道路段
                    if match_node[i][1] == up_node[j]:
                        export_rdsect.append(rdsectid[j])
                    if match_node[i][1] == down_node[j]:

                        length = alarm_rdsect[alarm_rdsect['rdsectid'] == rdsectid[j]].iloc[0, 6]
                        dir_desc = alarm_rdsect[alarm_rdsect['rdsectid'] == rdsectid[j]].iloc[0, 3]
                        if len(match_plan_sl_info) > 0:
                            add_coor_mark = add_coor(grouped_dyna_plan_sl, up_node[j], down_node[j], length)
                        else:
                            add_coor_mark = False
                        coor_mark.append([dir_desc, rdsectid[j], add_coor_mark])
                        # print([dir_desc, rdsectid[j], add_coor_mark])
                        # input()
                        # print(add_coor_mark)
                        import_rdsect.append(rdsectid[j])
                for j in range(len(coor_up_node)):
                    # 属于该节点的路段，出口为出口道路段
                    # print(match_node[i][1], coor_up_node[j])
                    # 匹配节点处所以协调方案
                    if match_node[i][1] == coor_up_node[j]:
                        coor_up_match.append([coor_up_node[j], coor_down_node[j], coor_rdsectid[j], coor_up_phase[j]])
                    if match_node[i][1] == coor_down_node[j]:
                        coor_down_match.append([coor_up_node[j], coor_down_node[j], coor_rdsectid[j], coor_down_phase[j]])
                # 节点处所有协调方案
                all_coor = coor_up_match+coor_down_match
                # print(match_node[i][1], export_rdsect, import_rdsect)
                export_desc = []
                import_desc = []
                coor_match_rdsect = []
                match_rdsect_num = 0
                # 匹配拥堵路段的进出口描述及正反向协调策略
                sign_coor = len(all_coor)
                for j in range(len(all_coor)):
                    desc = ""
                    get_rdsectid = all_coor[j][2]
                    # print(all_coor[j])
                    match_rdsect1 = alarm_rdsect[alarm_rdsect['rdsectid'] == get_rdsectid]
                    if len(match_rdsect1) > 0:
                        # print(match_rdsect1)
                        if match_node[i][1] == all_coor[j][0]:
                            desc = match_rdsect1.iloc[0, 4]
                        if match_node[i][1] == all_coor[j][1]:
                            desc = match_rdsect1.iloc[0, 3]
                        all_coor[j].append(desc)
                # print(all_coor)

                for m in export_rdsect:
                    # alarm_rdsect 为所在路段的id
                    # print(export_rdsect)
                    match_rdsect = alarm_rdsect[alarm_rdsect['rdsectid'] == m]
                    if len(match_rdsect) > 0:
                        # print(match_rdsect)
                        up_node1 = match_rdsect.iloc[0, 1]
                        down_node1 = match_rdsect.iloc[0, 2]
                        desc = match_rdsect.iloc[0, 4]
                        export_desc.append(desc)

                for m in import_rdsect:
                    match_rdsect = alarm_rdsect[alarm_rdsect['rdsectid'] == m]
                    up_node1 = match_rdsect.iloc[0, 1]
                    down_node1 = match_rdsect.iloc[0, 2]
                    desc = match_rdsect.iloc[0, 3]
                    import_desc.append(desc)
                #     print(coor_match_rdsect)  # [['519', '76', 'UTRSS001114', 'D']]
                #     coor_match_rdsect = excess_list(coor_match_rdsect)
                if len(export_desc) > 0:
                    for n in range(len(export_desc)):
                        # print(export_desc[n])
                        # print(alarm_times_grouped[match_node[i][1]][export_desc[n]])
                        times = alarm_times_grouped[match_node[i][1]][export_desc[n]]
                        result = {'scats_id': match_node[i][1], 'veh_desc': export_desc[n],
                                  'rdsectid': export_rdsect[n], 'alarm_times': times}
                        alarm_data.append([match_node[i][1], export_desc[n], export_rdsect[n], times])

                if len(import_desc) > 0:
                    for n in range(len(import_desc)):
                        # print(alarm_times_grouped[match_node[i][1]][import_desc[n]])
                        times = alarm_times_grouped[match_node[i][1]][import_desc[n]]
                        result = {'scats_id': match_node[i][1], 'veh_desc': import_desc[n],
                                  'rdsectid': import_rdsect[n], 'alarm_times': times}
                        alarm_data.append([match_node[i][1], import_desc[n], import_rdsect[n], times])
                # print(alarm_data)

                if len(coor_export_rdsect) + len(coor_import_rdsect) > 0:
                    print('%s节点匹配到协调方案' % match_node[i][1])
                    coor_cycle = 1
                else:
                    pass
                # 随机主要车流方向组合
                default_flow = '1'
                scats_id = match_node[i][1]
                # print(phase_dir)
                json_data = main_flow(conn, alarm_data, scats_id, alarm_times_grouped, all_coor,
                                      new_phase_match_data, new_lane_inf, sign_coor, coor_mark, phase_dir)
                # print(json_data)
                json_send(start_date,  start_time, end_time, conn, scats_id, json_data)

        conn.close()


if __name__ == '__main__':
    # conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
    #                         user=GlobalContent.pg_database72_research['user'],
    #                         password=GlobalContent.pg_database72_research['password'],
    #                         host=GlobalContent.pg_database72_research['host'],
    #                         port=GlobalContent.pg_database72_research['port'])
    # data = call_phase_match(conn, '519')
    # print(data)
    # phase_inf_combine('1')
    # call_phase_match()
    # call_alarm_times('2018-04-01', '2018-04-10', '07:00:00',  '09:00:00')
    main('2018-07-018', '08:11:00',  '08:16:00', 2)
