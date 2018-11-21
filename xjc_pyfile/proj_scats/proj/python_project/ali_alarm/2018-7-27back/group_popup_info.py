#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import cx_Oracle
import pandas as pd
import GlobalContent
# import GlobalContent
from collections import Counter
import json
import re


def call_postgre(str_date, str_time, start_time, end_time, start_date):
    frame = pd.DataFrame({})
    alarm = pd.DataFrame({})
    offset = pd.DataFrame({})
    phase = pd.DataFrame({})
    # alarm group roadsect info
    sql = "select group_id, alarm_date, time_point, rdsectid, up_node, down_node" \
          " from {0} where alarm_date = '{1}' and time_point = '{2}'" \
        .format(GlobalContent.alarm_group_rdsectid, str_date, str_time)
    # int alarm direction data
    sql2 = "select scats_id, vehicle_dir, is_entrance, delay_value from {0} where date_day " \
           "= '{1}' and time_point between '{2}' and '{3}'"\
        .format(GlobalContent.alarm_record, start_date, start_time, end_time)
    sql3 = "select c.*, d.upstream_phasename, d.upstream_number, d.hight_diffcycle, d.dowmstream_phasename, " \
           "d.downstream_number from (select b.segment_id, a.route_id from(select route_id, start_time, end_time " \
           "from {0} where start_time < '{4}' and end_time > '{3}')a " \
           "left join {1} b on a.route_id = b.route_id order by route_id, segment_id ) c " \
           "left join {2} d on c.segment_id = d.segment_id where c.segment_id is not null and " \
           "upstream_phasename is not null order by downstream_number"\
        .format(GlobalContent.coor_route, GlobalContent.coor_route_segment_rel, GlobalContent.coor_phase_diff,
                start_time, end_time)
    sql4 = "select scats_id, phase_name from {0}".format('col_control_phase')

    try:
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
        try:
            cr = conn.cursor()
            cr.execute(sql)
            rows = cr.fetchall()
            frame = pd.DataFrame(rows)
            frame.columns = ['group_id', 'alarm_date', 'time_point', 'rdsectid', 'up_node', 'down_node']
            cr.close()
        except Exception as e:
            print('弹窗基础数据：获取数据-以路段形式存放组团划分结果-失败: ' + str(e))
        try:
            cr = conn.cursor()
            cr.execute(sql2)
            rows = cr.fetchall()
            alarm = pd.DataFrame(rows)
            alarm.columns = ['scats_id', 'vehicle_dir', 'is_entrance', 'delay_value']
            cr.close()
        except Exception as e:
            print('弹窗基础数据：获取数据-报警记录-失败: ' + str(e))
        try:
            cr = conn.cursor()
            cr.execute(sql3)
            rows = cr.fetchall()
            offset = pd.DataFrame(rows)
            offset.columns = ['segment_id', 'route_id', 'upstream_phasename', 'upstream_number', 'hight_diffcycle',
                              'dowmstream_phasename', 'downstream_number']
            cr.close()
        except Exception as e:
            print('弹窗基础数据：获取数据-协调相位差-失败: ' + str(e))
        try:
            cr = conn.cursor()
            cr.execute(sql4)
            rows = cr.fetchall()
            phase = pd.DataFrame(rows)
            phase.columns = ['scats_id', 'phase_name']
            cr.close()
        except Exception as e:
            print('弹窗基础数据：获取数据-绿信比-失败: ' + str(e))
        conn.close()
    except Exception as e:
        print('弹窗基础数据：获取数据-连接postgre失败: ' + str(e))

    return frame, alarm, offset, phase


def call_postgre_group(start_time, end_time, start_date, int_tuple):
    match_group_alarm = pd.DataFrame({})
    sql = "select count(*) int_alarm_count, scats_id from {0} where date_day = '{1}' " \
          "and time_point between '{2}' and '{3}' and scats_id in {4} group by scats_id"\
        .format(GlobalContent.alarm_record, start_date, start_time, end_time, int_tuple)
    try:
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
        try:
            cr = conn.cursor()
            cr.execute(sql)
            rows = cr.fetchall()
            match_group_alarm = pd.DataFrame(rows)
            match_group_alarm.columns = ['int_alarm_count', 'scats_id']
            cr.close()
        except Exception as e:
            print('弹窗基础数据：获取数据-组团报警统计结果-失败: ' + str(e))
        conn.close()
    except Exception as e:
        print('弹窗基础数据：获取数据-连接postgre失败: ' + str(e))
    return match_group_alarm


def call_oracle(start_time, end_time, start_date):
    match_plan_sl_info = pd.DataFrame({})
    rows = []
    sql = "select * from {0} where FSTR_DATE={1} and FSTR_CYCLE_STARTTIME between " \
          "'{2}' and '{3}' ORDER BY FSTR_INTERSECTID, FSTR_DATE, FSTR_CYCLE_STARTTIME" \
        .format(GlobalContent.dyna_plan_selection, start_date, start_time, end_time)
    try:
        db = cx_Oracle.connect(GlobalContent.OracleUser)
        try:
            cr = db.cursor()
            cr.execute(sql)
            rows = cr.fetchall()

            cr.close()
        except Exception as e:
            print('弹窗基础数据：获取数据-路口基础绿信比方案数据-失败: ' + str(e))
        db.close()
    except Exception as e:
        print('弹窗基础数据：获取数据-连接oracle失败: ' + str(e))
    else:
        if len(rows) > 0:
            match_plan_sl_info = pd.DataFrame(rows)
            match_plan_sl_info.columns = ['FSTR_INTERSECTID', 'FSTR_DATE', 'FSTR_CYCLE_STARTTIME',
                                          'FSTR_CYCLE_LENGTH', 'PHASE_A', 'PHASE_B', 'PHASE_C', 'PHASE_D',
                                          'PHASE_E', 'PHASE_F', 'PHASE_G']
    return match_plan_sl_info


def order_by_delay(int_list, rd_dir, match_alarm_records):
    grouped_alarm = match_alarm_records.groupby(['scats_id'])
    int_dict = {}
    collision_int_order = []
    if rd_dir == 'in' or rd_dir == 'all_in':
        for int in int_list:
            one_int_alarm = grouped_alarm.get_group(int)
            delay_list = one_int_alarm[(one_int_alarm.is_entrance == 1)]['delay_value'].tolist()
            cur_describe = pd.DataFrame(delay_list).describe(percentiles=[.75])
            delay_value = cur_describe.loc['75%', 0]
            int_dict[delay_value] = int
        a = [(k, int_dict[k]) for k in sorted(int_dict.keys(), reverse=True)]
        for new_int in a:
            collision_int_order.append(new_int[1])
    elif rd_dir == 'out' or rd_dir == 'all_out':
        for int in int_list:
            one_int_alarm = grouped_alarm.get_group(int)
            delay_list = one_int_alarm[(one_int_alarm.is_entrance == 0)]['delay_value'].tolist()
            cur_describe = pd.DataFrame(delay_list).describe(percentiles=[.75])
            delay_value = cur_describe.loc['75%', 0]
            int_dict[delay_value] = int
        a = [(k, int_dict[k]) for k in sorted(int_dict.keys(), reverse=True)]
        for new_int in a:
            collision_int_order.append(new_int[1])
    elif len(rd_dir) > 3 and rd_dir[-4:-1] == 'out':
        for int in int_list:
            one_int_alarm = grouped_alarm.get_group(int)
            alarm_num = len(one_int_alarm)
            int_dict[alarm_num] = int
        a = [(k, int_dict[k]) for k in sorted(int_dict.keys(), reverse=True)]
        for new_int in a:
            collision_int_order.append(new_int[1])
    else:
        for int in int_list:
            one_int_alarm = grouped_alarm.get_group(int)
            alarm_num = len(one_int_alarm)
            int_dict[alarm_num] = int
        a = [(k, int_dict[k]) for k in sorted(int_dict.keys(), reverse=True)]
        for new_int in a:
            collision_int_order.append(new_int[1])
    return collision_int_order


def next_int(int, up, down, int_list, one_group_data, group_int_order, match_alarm_records, endloop):
    # print('cur_int' + str(int))
    if int in up:
        # next_index = up.index(first_int)
        # print('in_up')
        next_index_list = one_group_data[one_group_data.up_node == int].index.tolist()
        # print(next_index_list)
        new_next_index_list = []
        for m in range(len(next_index_list)):
            if one_group_data.loc[next_index_list[m], 'down_node'] not in group_int_order:
                new_next_index_list.append(next_index_list[m])
        # print(new_next_index_list)
        if len(new_next_index_list) == 1:
            group_int_order.append(one_group_data.loc[new_next_index_list[0], 'down_node'])
        elif len(new_next_index_list) > 1:
            collision_int_list = []
            for i in range(len(new_next_index_list)):
                collision_int_list.append(one_group_data.loc[new_next_index_list[i], 'down_node'])
            collision_int_order = order_by_delay(collision_int_list, 'in', match_alarm_records)
            group_int_order.append(collision_int_order[0])
        else:
            endloop = 1
        # loc = 'up'
    elif int in down:
        # print('in_down')

        next_index_list = one_group_data[one_group_data.down_node == int].index.tolist()
        # print(next_index_list)

        # del_list = []
        new_next_index_list = []
        for m in range(len(next_index_list)):
            if one_group_data.loc[next_index_list[m], 'up_node'] not in group_int_order:
                new_next_index_list.append(next_index_list[m])

        # print(new_next_index_list)
        if len(new_next_index_list) == 1:
            group_int_order.append(one_group_data.loc[new_next_index_list[0], 'up_node'])
        elif len(new_next_index_list) > 1:
            collision_int_list = []
            for i in range(len(new_next_index_list)):
                collision_int_list.append(one_group_data.loc[new_next_index_list[i], 'up_node'])
            collision_int_order = order_by_delay(collision_int_list, 'out', match_alarm_records)
            group_int_order.append(collision_int_order[0])
        else:
            endloop = 1

    if group_int_order.count(group_int_order[-1]) > 1:
        del group_int_order[-1]
        endloop = 1
    if len(int_list) == len(group_int_order):
        endloop = 1
    # print('next_int' + str(group_int_order[-1]))
    return group_int_order, endloop


def int_order(one_group_data, match_alarm_records):
    up = one_group_data['up_node'].tolist()
    down = one_group_data['down_node'].tolist()
    all = up + down
    int_list = list(set(all))  # ints contained in the group
    group_int_order = []
    int_rd_num_all = []
    # print(one_group_data)
    for int in int_list:
        int_rd_num_item = []
        # print(int)
        if all.count(int) == 1:
            int_rd_num_item.append(int)
            int_rd_num_item.append(1)
            if int in up:
                int_rd_num_item.append('out')
            elif int in down:
                int_rd_num_item.append('in')
            int_rd_num_all.append(int_rd_num_item)
        elif all.count(int) == 2:
            int_rd_num_item.append(int)
            int_rd_num_item.append(2)
            if int in up and int not in down:
                int_rd_num_item.append('all-out')
            elif int in down and int not in up:
                int_rd_num_item.append('all-in')
            else:
                int_rd_num_item.append('in-out')
            int_rd_num_all.append(int_rd_num_item)
        elif all.count(int) > 2:
            int_rd_num_item.append(int)
            int_rd_num_item.append(all.count(int))
            in_num = down.count(int)
            out_num = up.count(int)
            int_rd_num_item.append(str(in_num) + 'in-' + str(out_num) + 'out')
            int_rd_num_all.append(int_rd_num_item)
    count_result_df = pd.DataFrame(int_rd_num_all, columns=['scats_id', 'num', 'dir'])  # determine int type
    grouped_type = count_result_df.groupby(['num', 'dir'])
    # print(count_result_df)
    if len(int_list) <= 3:
        for type in grouped_type.groups:
            one_type_data = grouped_type.get_group(type)
            # print(one_type_data)
            if type[1] == 'in':
                if len(one_type_data) == 1:
                    group_int_order.append(one_type_data.iloc[0]['scats_id'])
                elif len(one_type_data) > 1:  # int order
                    collision_int_list = one_type_data['scats_id'].tolist()
                    rd_dir = type[1]
                    collision_int_order = order_by_delay(collision_int_list, rd_dir, match_alarm_records)  # order by delay
                    for x in collision_int_order:
                        group_int_order.append(x)
            elif type[1] == 'out':
                if len(one_type_data) == 1:
                    group_int_order.append(one_type_data.iloc[0]['scats_id'])
                elif len(one_type_data) > 1:
                    collision_int_list = one_type_data['scats_id'].tolist()
                    rd_dir = type[1]
                    collision_int_order = order_by_delay(collision_int_list, rd_dir, match_alarm_records)
                    for x in collision_int_order:
                        group_int_order.append(x)
            elif type[1] == 'all-in':
                if len(one_type_data) == 1:
                    group_int_order.append(one_type_data.iloc[0]['scats_id'])
                elif len(one_type_data) > 1:
                    collision_int_list = one_type_data['scats_id'].tolist()
                    rd_dir = type[1]
                    collision_int_order = order_by_delay(collision_int_list, rd_dir, match_alarm_records)
                    for x in collision_int_order:
                        group_int_order.append(x)
            elif type[1] == 'all-out':
                if len(one_type_data) == 1:
                    group_int_order.append(one_type_data.iloc[0]['scats_id'])
                elif len(one_type_data) > 1:
                    collision_int_list = one_type_data['scats_id'].tolist()
                    rd_dir = type[1]
                    collision_int_order = order_by_delay(collision_int_list, rd_dir, match_alarm_records)
                    for x in collision_int_order:
                        group_int_order.append(x)
            elif type[1] == 'in-out':
                if len(one_type_data) == 1:
                    group_int_order.append(one_type_data.iloc[0]['scats_id'])
                elif len(one_type_data) > 1:
                    # print(int)
                    collision_int_list = one_type_data['scats_id'].tolist()
                    rd_dir = type[1]
                    collision_int_order = order_by_delay(collision_int_list, rd_dir, match_alarm_records)
                    for x in collision_int_order:
                        group_int_order.append(x)
            else:
                if len(one_type_data) == 1:
                    group_int_order.append(one_type_data.iloc[0]['scats_id'])
                elif len(one_type_data) > 1:
                    collision_int_list = one_type_data['scats_id'].tolist()
                    rd_dir = type[1]
                    collision_int_order = order_by_delay(collision_int_list, rd_dir, match_alarm_records)
                    for x in collision_int_order:
                        group_int_order.append(x)
    else:
        one_dir_int = []
        pair = []
        for m in range(len(one_group_data)):
            pair.append({'up': one_group_data.iloc[m]['up_node'], 'down': one_group_data.iloc[m]['down_node']})
        temp_pair = []
        for m in range(len(pair)):
            for n in range(len(pair)):
                if pair[m]['up'] == pair[n]['down'] and pair[m]['down'] == pair[n]['up']:
                    temp_pair.append(pair[m]['up'])
                    temp_pair.append(pair[m]['down'])
        for m in range(len(temp_pair)):
            if all.count(temp_pair[m]) == 2:
                one_dir_int.append(temp_pair[m])
        # print(one_dir_int)
        if one_dir_int:
            if len(one_dir_int) == 1:
                first_int = one_dir_int[0]
            else:
                collision_int_order = order_by_delay(one_dir_int, 'in', match_alarm_records)
                first_int = collision_int_order[0]
        else:  # no one dir in-out
            for int in int_list:
                if all.count(int) == 1:
                    one_dir_int.append(int)
            first_int_list = []
            for temp_int in one_dir_int:
                if count_result_df[count_result_df.scats_id == temp_int].iloc[0]['dir'] == 'in':
                    first_int_list.append(temp_int)
                    new_dir = 'in'
            if len(first_int_list) == 0:
                first_int_list = one_dir_int
                new_dir = 'out'
            if len(first_int_list) == 1:
                first_int = first_int_list[0]
            else:
                collision_int_order = order_by_delay(first_int_list, new_dir, match_alarm_records)
                first_int = collision_int_order[0]
        # print(first_int)
        # print('一拳超人')
        group_int_order.append(first_int)
        # input()
        endloop = 0
        int = first_int
        # print(endloop)
        # print('啊啊啊啊啊啊')
        while endloop == 0:
            group_int_order, endloop = next_int(int, up, down, int_list, one_group_data, group_int_order, match_alarm_records, endloop)
            int = group_int_order[-1]
            # input()
        # print(group_int_order)
        difference = [v for v in int_list if v not in group_int_order]
        for left_int in difference:
            group_int_order.append(left_int)
        # print(int_list)
        # print(group_int_order)

    return int_list, group_int_order


def cause_analysis(int_list, match_group_alarm, one_group_data, start_time, end_time):
    item = {}
    int_num = len(int_list)  # 1.
    group_alarm_num = sum(match_group_alarm['int_alarm_count'].tolist())  # 2.
    max_alarm = max(match_group_alarm['int_alarm_count'].tolist())
    index = match_group_alarm['int_alarm_count'].tolist().index(max_alarm)
    # print(index)
    max_alarm_int = match_group_alarm.iloc[index]['scats_id']  # 3.
    max_alarm_int_num = match_group_alarm.iloc[index]['int_alarm_count']
    per = round(100 * max_alarm_int_num / group_alarm_num, 2)
    # print(max_alarm_int)
    rds_num = len(one_group_data)  # 4.
    if GlobalContent.lang == 'en':
        test1 = 'The group contains %d intersections and %d road sections'
        test2 = 'During %s and %s, alarm happened %d times in the group'
        test3 = 'Intersect %s account for %d percent in all alarms'
        # test4 = 'The group contains %d road sections'
    else:
        test1 = '组团中包含%d个路口，%d个路段'
        test2 = '%s至%s间组团中路口共发生%d次报警'
        test3 = '路口%s报警次数占总数的百分之%d'
        # test4 = 'The group contains %d road sections'
    as1 = test1 %(int_num, rds_num)
    as2 = test2 %(start_time, end_time, group_alarm_num)
    as3 = test3 %(max_alarm_int, per)
    # as4 = test4 %(rds_num)
    item = {'cause1': as1, 'cause2': as2, 'cause3': as3}
    return item


def int_basic_information(scats_id, grouped_dyna_plan_sl, match_offset_record, int_alarm_num, no_plan_sl_data,
                          plan_sl_data_exist, int_phase_list):
    # cycle = 0
    int_item = []
    int_item.append({'scats_id': scats_id})
    try:
        one_int_sl = grouped_dyna_plan_sl.get_group(scats_id)
        # print(one_int_sl)
        cycle = int(Counter(one_int_sl['FSTR_CYCLE_LENGTH'].tolist()).most_common(1)[0][0])
        grouped_split = one_int_sl.groupby(['PHASE_A', 'PHASE_B', 'PHASE_C', 'PHASE_D',
                                            'PHASE_E', 'PHASE_F', 'PHASE_G']).count().reset_index()
        index = grouped_split['FSTR_INTERSECTID'].tolist() \
            .index(max(grouped_split['FSTR_INTERSECTID'].tolist()))
        split_plan = {'A': str(grouped_split.iloc[index]['PHASE_A']),
                      'B': str(grouped_split.iloc[index]['PHASE_B']),
                      'C': str(grouped_split.iloc[index]['PHASE_C']),
                      'D': str(grouped_split.iloc[index]['PHASE_D']),
                      'E': str(grouped_split.iloc[index]['PHASE_E']),
                      'F': str(grouped_split.iloc[index]['PHASE_F']),
                      'G': str(grouped_split.iloc[index]['PHASE_G'])}
        int_item.append(split_plan)
        plan_sl_data_exist.append(scats_id)
    except KeyError:
        no_plan_sl_data.append(scats_id)
        cycle = ''
        int_item.append({'A': '100', 'B': '100', 'C': '100', 'D': '100', 'E': '100', 'F': '100', 'G': '100'})
    temp_phase_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    for m in temp_phase_list:
        if m in int_phase_list:
            if int_item[1][m] == '0':
                int_item[1][m] = '1'
        else:
            int_item[1][m] = ''
    # print(int_item[1])
    # offset plan
    if not match_offset_record.empty:
        if scats_id in match_offset_record['downstream_number'].tolist():
            # print('bbb')
            grouped_offset = match_offset_record.groupby(['downstream_number'])
            one_int_offset = grouped_offset.get_group(scats_id)
            # print(one_int_offset)
            offset = ''
            upstream = ''
            downstream = ''
            for m in range(len(one_int_offset)):
                if m == 0:
                    offset = str(one_int_offset.iloc[m]['hight_diffcycle'])
                    upstream = str(one_int_offset.iloc[m]['upstream_number']) + \
                               str(one_int_offset.iloc[m]['upstream_phasename'])
                    downstream = str(one_int_offset.iloc[m]['downstream_number']) + \
                                 str(one_int_offset.iloc[m]['dowmstream_phasename'])
                else:
                    offset = offset + ' | ' + str(one_int_offset.iloc[m]['hight_diffcycle'])
                    upstream = upstream + ' | ' + str(one_int_offset.iloc[m]['upstream_number']) + \
                               str(one_int_offset.iloc[m]['upstream_phasename'])
                    downstream = downstream + ' | ' + str(one_int_offset.iloc[m]['downstream_number']) + \
                                 str(one_int_offset.iloc[m]['dowmstream_phasename'])
            int_item.append({'offset': offset, 'upstream': upstream, 'downstream': downstream,
                             'cycle': cycle, 'alarm': str(int_alarm_num)})
        else:
            int_item.append({'offset': '', 'upstream': '', 'downstream': '', 'cycle': cycle,
                             'alarm': str(int_alarm_num)})
    else:
        int_item.append({'offset': '', 'upstream': '', 'downstream': '', 'cycle': cycle,
                         'alarm': str(int_alarm_num)})

    return int_item, no_plan_sl_data, plan_sl_data_exist


def no_run_data_int_basic_info(scats_id, phase_list, int_basic_info_dict, match_offset_record):
    temp_phase_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    for p in temp_phase_list:
        if p not in phase_list:
            int_basic_info_dict[-1][1][p] = ''
    if not match_offset_record.empty:
        if scats_id in match_offset_record['downstream_number'].tolist():
            grouped_offset = match_offset_record.groupby(['downstream_number'])
            one_int_offset = grouped_offset.get_group(scats_id)
            offset = ''
            upstream = ''
            downstream = ''
            for m in range(len(one_int_offset)):
                if m == 0:
                    offset = str(one_int_offset.iloc[m]['hight_diffcycle'])
                    upstream = str(one_int_offset.iloc[m]['upstream_number']) + \
                               str(one_int_offset.iloc[m]['upstream_phasename'])
                    downstream = str(one_int_offset.iloc[m]['downstream_number']) + \
                                 str(one_int_offset.iloc[m]['dowmstream_phasename'])
                else:
                    offset = offset + ' | ' + str(one_int_offset.iloc[m]['hight_diffcycle'])
                    upstream = upstream + ' | ' + str(one_int_offset.iloc[m]['upstream_number']) + \
                               str(one_int_offset.iloc[m]['upstream_phasename'])
                    downstream = downstream + ' | ' + str(one_int_offset.iloc[m]['downstream_number']) + \
                                 str(one_int_offset.iloc[m]['dowmstream_phasename'])
            int_basic_info_dict[-1][2]['offset'] = offset
            int_basic_info_dict[-1][2]['upstream'] = upstream
            int_basic_info_dict[-1][2]['downstream'] = downstream
    return int_basic_info_dict


# 发送json格式数据
def json_send(data_type, sub_id, var_data, start_date, end_date, start_time, end_time):
    sql_delete = "delete from {0} where data_type ='{1}' and sub_id ='{2}' and start_date ='{3}' and end_date ='{4}' " \
                 "and start_time='{5}' and end_time ='{6}'"\
        .format(GlobalContent.alarm_transfer_json_data, data_type, sub_id, start_date, end_date, start_time, end_time)
    sql_send = "insert into {0}(data_type,sub_id,start_date,end_date,start_time,end_time,json_data) " \
               "values(%s,%s,%s,%s,%s,%s,%s)".format(GlobalContent.alarm_transfer_json_data)
    try:  # 数据库连接超时
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
    except Exception as e:
        print('json_send', e)
    else:
        try:
            cr.execute(sql_send, (data_type, sub_id, start_date, end_date, start_time, end_time, var_data))
            conn.commit()
        except psycopg2.IntegrityError:
            conn.commit()
            cr.execute(sql_delete)
            conn.commit()
            cr.execute(sql_send, (data_type, sub_id, start_date, end_date, start_time, end_time, var_data))
            # print('json_send 数据重复，已删除并重新插入数据')
            conn.commit()

        cr.close()
        conn.close()


# 是否新增协调方案
def add_coor(str_date, str_time, up_scatsid, down_scatsid, rds_length):
    if_add_coor = False
    start_time = str_time[:8]
    end_time = str_time[-8:]
    start_date = str_date[:10]
    end_date = str_date[-10:]
    match_plan_sl_info = call_oracle(start_time, end_time, start_date)
    grouped_dyna_plan_sl = match_plan_sl_info.groupby(['FSTR_INTERSECTID'])
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


def main(start_date, start_time, end_time):
    # start_time = str_time[:8]
    # end_time = str_time[-8:]
    # start_date = str_date[:10]
    # end_date = str_date[-10:]
    no_plan_sl_data = []
    plan_sl_data_exist = []
    group_int_order = []
    sub_order = []
    match_rds_records, match_alarm_records, match_offset_record, match_phase_record = \
        call_postgre(start_date , start_time + '~' + end_time, start_time, end_time, start_date)
    match_plan_sl_info = call_oracle(start_time, end_time, start_date )
    # print(match_offset_record)
    print('弹窗基础数据部分：' + start_date + ', ' + start_time + '~' + end_time)
    if len(match_rds_records) > 0:
        grouped_data = match_rds_records.groupby(['group_id'])
        if not match_rds_records.empty and not match_alarm_records.empty:
            for group_id in grouped_data.groups:
                # print(group_id)
                one_group_data = grouped_data.get_group(group_id)
                group_information = {}
                int_basic_info_dict = []
                int_list, group_int_order = int_order(one_group_data, match_alarm_records)
                order = {'order': group_int_order}
                # cause analysis
                int_tuple = tuple(int_list)
                match_group_alarm = call_postgre_group(start_time, end_time, start_date, int_tuple)
                cause_dict = cause_analysis(int_list, match_group_alarm, one_group_data, start_time, end_time)
                group_information['cause_analysis'] = cause_dict  # output cause analysis
                group_information['regulation_order'] = order  # output regulation order

                # print(match_plan_sl_info)
                if not match_plan_sl_info.empty:
                    grouped_dyna_plan_sl = match_plan_sl_info.groupby(['FSTR_INTERSECTID'])
                    for scats_id in group_int_order:
                        phase_list = match_phase_record[match_phase_record.scats_id == scats_id]['phase_name'].tolist()  # 当前路口在pg中的相位
                        int_alarm_num_index = match_group_alarm[match_group_alarm.scats_id == scats_id].index.tolist()[0]
                        int_alarm_num = match_group_alarm.iloc[int_alarm_num_index]['int_alarm_count']
                        int_item, no_plan_sl_data, plan_sl_data_exist = \
                            int_basic_information(scats_id, grouped_dyna_plan_sl, match_offset_record, int_alarm_num,
                                                  no_plan_sl_data, plan_sl_data_exist, phase_list)
                        int_basic_info_dict.append(int_item)
                else:
                    for scats_id in group_int_order:
                        no_plan_sl_data.append(scats_id)
                        int_alarm_num_index = match_group_alarm[match_group_alarm.scats_id == scats_id].index.tolist()[0]
                        int_alarm_num = match_group_alarm.iloc[int_alarm_num_index]['int_alarm_count']
                        int_basic_info_dict.append([{'scats_id': scats_id},
                                                   {"A": "100", "B": "100", "C": "100", "D": "100",
                                                    "E": "100", "F": "100", "G": "100"},
                                                   {"offset": "", "upstream": "", "downstream": "",
                                                    "cycle": "", "alarm": str(int_alarm_num)}])
                        phase_list = match_phase_record[match_phase_record.scats_id == scats_id]['phase_name'].tolist()  # 当前路口在pg中的相位
                        int_basic_info_dict = no_run_data_int_basic_info(scats_id, phase_list, int_basic_info_dict, match_offset_record)

                group_information['int_basic_info'] = int_basic_info_dict
                # print(int_basic_info_dict)

                var_data = json.dumps(group_information, ensure_ascii=False)
                # print(var_data)
                data_type = "11"  # 11: group basic information
                sub_id = re.split(r'[_]', group_id)[0] + re.split(r'[_]', group_id)[-1]
                json_send(data_type, sub_id, var_data, start_date.replace("-", ""), start_date.replace("-", ""), start_time, end_time)
                sub_order.append([sub_id, group_int_order])
        else:
            pass

    # if no_plan_sl_data:
    #     print('弹窗基础数据：' + 'int' + str(no_plan_sl_data) + '无方案选择数据')
    print('弹窗基础数据部分：计算结束')
    return sub_order


if __name__ == '__main__':
    main('2018-07-02', '07:40:00', '08:10:00')
    # add_coor('2018-04-13~2018-04-13', '07:00:00~08:00:00', '2', '1', '190')
