#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
import pandas as pd
import GlobalContent
import numpy as np
import datetime as dt
import json


class Content:
    alarm_group_rdsectid = 'alarm_group_rdsectid'
    alarm_group_node = 'alarm_group'
    alarm_transfer_json_data = 'alarm_transfer_json_data'
    pass


def call_key_node(conn, start_date, end_date, start_time, end_time):
    date_interval = start_date + '~' + end_date
    time_interval = start_time + '~' + end_time
    rows = []
    sql = "select group_id, key_node from {0} where alarm_date = '{1}' and time_point = '{2}'"\
        .format(GlobalContent.alarm_keynode, date_interval, time_interval)
    try:
        cr = conn.cursor()
        cr.execute(sql)
        rows = cr.fetchall()
    except Exception as e:
        print("call_key_node:", e)

    return rows



def call_int_name(conn):
    rows = []
    sql = "select scats_id,inter_name, english_inter_name from {0} where inter_name is not null".format(GlobalContent.inter_info)
    try:
        cr = conn.cursor()
        cr.execute(sql)
        rows = cr.fetchall()
    except Exception as e:
        print("call_int_name:", e)
    return rows


# 获取组团路段信息
def call_postgres(conn, start_date, end_date, start_time, end_time):
    date_interval = start_date + '~' + end_date
    time_interval = start_time + '~' + end_time
    frame = pd.DataFrame({})
    match_alarm_records = pd.DataFrame({})
    try:  # 数据库连接超时

        cr = conn.cursor()
        sql = "select group_id, rdsectid, up_node, down_node from {0} where alarm_date = '{1}' and time_point ='{2}'"\
            .format(Content.alarm_group_rdsectid, date_interval, time_interval)
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


# 获取协调方向信息
def call_coor_dir(conn):
    sql = "SELECT c.start_time,c.end_time,c.upstream_nodeid,c.downstream_nodeid,c.segment_dir,c.upstream_phasename,d.rdsectid from" \
          "(SELECT a.*,b.segment_dir " \
          "FROM(SELECT k.start_time,k.end_time,k.route_id,k.segment_id,k.route_name,t.upstream_nodeid," \
          "t.downstream_nodeid,t.upstream_phasename " \
          "FROM(select m.start_time,m.end_time,m.route_id,m.route_name,n.segment_id " \
          "from coor_route m ,coor_route_segment_rel n where m.route_id = n.route_id) K,coor_phase_diff t " \
          "where k.segment_id=t.segment_id) a LEFT JOIN coor_route_segment b on a.segment_id = b.segment_id) c" \
          "	left join pe_tobj_roadsect d on c.upstream_nodeid=d.new_up_nod and c.downstream_nodeid=d.new_down_n " \
          "ORDER BY c.route_id"
    # date_interval = start_date + '~' + end_date
    # time_interval = start_time + '~' + end_time
    frame = pd.DataFrame({})
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
            i[0] = dt.time.strftime(i[0], '%H:%M:%S')
            i[1] = dt.time.strftime(i[1], '%H:%M:%S')
        # print(rows)
        frame = pd.DataFrame(rows_list)
        frame.columns = ['start_time', 'end_time', 'up_node', 'down_node', 'coor_dir','phase', 'rdsectid']
        cr.close()

    except Exception as e:
        print(e)

    # print(frame)
    return frame


# 获取节点配置信息
def call_node_inf(conn):
    sql = "select nodeid,systemid from pe_tobj_node "
    rows_list = []
    try:  # 数据库连接超时
        # conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
        #                         user=GlobalContent.pg_database72_research['user'],
        #                         password=GlobalContent.pg_database72_research['password'],
        #                         host=GlobalContent.pg_database72_research['host'],
        #                         port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
        cr.execute(sql)
        rows = cr.fetchall()
        rows_list = []
        for i in rows:
            i = list(i)
            rows_list.append(i)
        # print(rows_list)
        cr.close()
    except Exception as e:
        print(e)
    # print(frame)
    return rows_list


# 获取组团节点信息
def call_group_node(conn, start_date, end_date, start_time, end_time):
    date_interval = start_date + '~' + end_date
    time_interval = start_time + '~' + end_time
    frame = pd.DataFrame({})
    rows_list = []
    sql = "select group_id ,scats_id,alarm_times from {0} where alarm_date = '{1}' and time_point ='{2}'"\
        .format(Content.alarm_group_node, date_interval, time_interval)
    try:
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
        print("call_group_node:", e)
    # print(frame)
    return rows_list


# 去除冗余，生成relation数据
def deleta_excess(data_list1, noid_inf):

    up_node = []
    relation = []
    down_node = []
    node_combine = []
    for i in range(len(data_list1)):
        if ([data_list1[i][0], data_list1[i][1]] not in node_combine) or \
                ([data_list1[i][1], data_list1[i][0]] not in node_combine):
            up_node.append(data_list1[i][0])
            down_node.append(data_list1[i][1])
            node_combine.append([data_list1[i][0], data_list1[i][1]])
            fromId = data_list1[i][0]
            toId = data_list1[i][1]
            if data_list1[i][2] == 1:
                l2 = True
            else:
                l2 = False
            if data_list1[i][3] == 1:
                r2 = True
            else:
                r2 = False
            if data_list1[i][4] == 1:
                l1 = True
            else:
                l1 = False
            if data_list1[i][5] == 1:
                r1 = True
            else:
                r1 = False
            for j in noid_inf:
                if fromId == j[1]:
                    fromId = j[0]
                if toId == j[1]:
                    toId = j[0]
            whichLine = {'l1': l1, 'l2': l2, 'r1': r1, 'r2': r2}
            busy_rdsect = {'fromId': fromId, 'toId': toId, 'whichLine': whichLine}
            relation.append(busy_rdsect)
    # print(relation)
    return relation


# 匹配报警次数,生成data
def alarm_times_match(group_data_node,noid_inf, group_key_node, int_name, int_order):
    data = []
    data_order = []
    # print(group_data_node)
    # print(noid_inf)
    # print(int_name)
    # print(group_key_node)
    # input()
    for i in range(len(group_data_node)):
        isKey = False
        name = '无'
        scats_id = group_data_node[i][1]
        alarm_times = group_data_node[i][2]
        nodeid = scats_id
        for j in noid_inf:
            if group_data_node[i][1] == j[1]:
                nodeid = j[0]
            else:
                pass
        for j in group_key_node:
            if scats_id == j[1]:
                isKey = True
        for j in int_name:

            if scats_id == j[0]:
                if j[2] is not None:
                    name = j[2]
                else:
                    name = j[1]
        alarm_times ={'id': nodeid, 'x': None, 'y': None, 'value': scats_id, 'isKey': isKey, 'name': name}
        data.append(alarm_times)
    for i in int_order:
        for j in data:
            if j['value']== i:
                data_order.append(j)
            else:
                pass
    # print(data_order)
    return data_order


# 发送json格式数据
def json_send(conn, data_type, sub_id, var_data, start_date, end_date, start_time, end_time):
    start_date = str(start_date.replace('-', ''))
    end_date = str(end_date.replace('-', ''))
    sql_delete = "delete from {0} where data_type ='{1}' and sub_id ='{2}' and start_date ='{3}' and end_date ='{4}' " \
                 "and start_time='{5}' and end_time ='{6}'"\
        .format(Content.alarm_transfer_json_data, data_type, sub_id, start_date, end_date, start_time, end_time)
    sql_send = "insert into {0}(data_type,sub_id,start_date,end_date,start_time,end_time,json_data) " \
               "values(%s,%s,%s,%s,%s,%s,%s)".format(Content.alarm_transfer_json_data)
    try:  # 数据库连接超时
        # conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
        #                         user=GlobalContent.pg_database72_research['user'],
        #                         password=GlobalContent.pg_database72_research['password'],
        #                         host=GlobalContent.pg_database72_research['host'],
        #                         port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
    except Exception as e:
        print('json_send', e)

    else:
        # print(sql_delete)
        # print(start_date, end_date)
        try:
            cr.execute(sql_send, (data_type, sub_id, start_date, end_date, start_time, end_time, var_data))
            conn.commit()
        except psycopg2.IntegrityError:
            conn.commit()
            cr.execute(sql_delete)
            conn.commit()
            cr.execute(sql_send, (data_type, sub_id, start_date, end_date, start_time, end_time, var_data))
            print('json_send 数据重复，已删除并重新插入数据')
            conn.commit()
        cr.close()


# 获取路段地理信息
def call_rdsect_geom(start_date, end_date, start_time, end_time):
    date_interval = start_date + '~' + end_date
    time_interval = start_time + '~' + end_time
    frame = pd.DataFrame({})
    rows_list = []
    rows_node_list = []
    rows_rdsect_list = []
    sql_node = "select group_id, scats_id, ST_AsText(geom) from {0} where alarm_date = '{1}' and time_point ='{2}'" \
        .format(Content.alarm_group_node, date_interval, time_interval)
    sql_rdsect = "select group_id, rdsectid,  ST_AsText(geom) from {0} where alarm_date = '{1}' and time_point ='{2}'"\
        .format(Content.alarm_group_rdsectid, date_interval, time_interval)

    try:  # 数据库连接超时
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
        cr.execute(sql_node)
        rows_node = cr.fetchall()

        cr.execute(sql_rdsect)
        rows_rdsect = cr.fetchall()
        # print(rows)
        rows_node_list = []
        for i in rows_node:
            i = list(i)
            rows_node_list.append(i)
        rows_rdsect_list = []
        for i in rows_rdsect:
            i = list(i)
            rows_rdsect_list.append(i)
        cr.close()
        conn.close()
    except Exception as e:
        print(e)
    # print(rows_node_list)
    # print(rows_rdsect_list)
    return rows_node_list, rows_rdsect_list


# 发送JSON地理信息数据
def geom_json(conn, start_date, end_date, start_time, end_time, noid_inf):
    if len(noid_inf)!=0:
        node_geom, rdsect_geom = call_rdsect_geom(start_date, end_date, start_time, end_time)
        group_id = []
        all_geom = []
        var_data = []
        [group_id.append(node[0]) for node in node_geom]
        group_id = set(group_id)
        group_id = sorted(group_id)
        # print(group_id)
        for i in group_id:
            scats_id = []
            node_id = []
            group_node = []
            group_rdsect = []
            for j in node_geom:
                if j[0] == i:
                    scats_id.append(j[1])
                    group_node.append(j[2])
            for m in rdsect_geom:
                if m[0] == i:

                    group_rdsect.append(m[2])
            i = i.replace('_', '')
            for m in scats_id:
                for n in noid_inf:
                    if n[1] == m:
                        node_id.append(n[0])

            group_data = {'Name': i, 'Data': {"NodeId": node_id, "NodeGeom": group_node, "RdsectGeom": group_rdsect}}
            # id = i[0:5]+i[-1:]
            all_geom.append(group_data)
            var_data = {"GeomData": all_geom}
            var_data = json.dumps(var_data, ensure_ascii=False)
    else:
        var_data = []

    data_type = "2"
    sub_id = "brain"
    json_send(conn, data_type, sub_id, var_data, start_date, end_date, start_time, end_time)
    # print(var_data)


# 主程序
def main(start_date, end_date, start_time, end_time, int_order):
    # print(int_order)
    dt_start = dt.datetime.strptime(start_time,"%H:%M:%S")
    dt_end = dt.datetime.strptime(end_time, "%H:%M:%S")
    # call_postgres('2018-03-05', '2018-03-15', '07:00:00', '09:00:00')
    group_data_node = []
    group_data_road = []
    try:
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
    except Exception as e:
        print('conn:', e)
    else:
        # 获取组团节点信息
        group_data_node = call_group_node(conn, start_date, end_date, start_time, end_time)
        # 获取组团路段信息
        group_data_road = call_postgres(conn, start_date, end_date, start_time, end_time)
        # 获取关键点信息
        group_key_node = call_key_node(conn, start_date, end_date, start_time, end_time)
        int_name = call_int_name(conn)

        if len(group_data_road) > 0:
            group_id_list = np.array(group_data_road['group_id']).tolist()
            group_id_list = set(group_id_list)
            group_id_list = sorted(group_id_list)
            # print(group_id_list)
            noid_inf = call_node_inf(conn)
            coor_dir = call_coor_dir(conn)
            # 匹配在时间段内生效的协调策略
            match_coor = coor_dir[((coor_dir['start_time'] > start_time) & (coor_dir['start_time'] < end_time)) |
                                  ((coor_dir['end_time'] > start_time) & (coor_dir['end_time'] < end_time)) |
                                  ((coor_dir['start_time'] < start_time) & (coor_dir['end_time'] > end_time))]
            # print(match_coor)
            # print(group_data_road)

            coor_dir_up = []
            coor_dir_down = []
            for indexs in match_coor.index:
                up_nodeid = match_coor.loc[indexs].values[2]
                down_nodeid = match_coor.loc[indexs].values[3]
                coor_rdsectid = match_coor.loc[indexs].values[3]
                for j in noid_inf:
                    if up_nodeid == j[0]:
                        coor_dir_up.append(j[1])
                    if down_nodeid == j[0]:
                        coor_dir_down.append(j[1])
            # print(group_id_list, int_order)
            for k in group_id_list:
                match_order = []
                for j in int_order:
                    if j[0] == k.replace('_',''):
                        match_order = j[1]
                # print(match_order, k)
                # input()
                match_data = group_data_road[group_data_road['group_id'] == k]
                match_node = []
                for j in group_data_node:
                    if j[0] == k:
                        match_node.append(j)
                up_node = np.array(match_data['up_node']).tolist()
                down_node = np.array(match_data['down_node']).tolist()
                rdsectid = np.array(match_data['rdsectid']).tolist()

                # print(coor_dir)
                # match_coor = coor_dir[((coor_dir['start_time'] > start_time) & (coor_dir['start_time'] < end_time)) |
                #                       ((coor_dir['end_time'] > start_time) & (coor_dir['end_time'] < end_time)) |
                #                       ((coor_dir['start_time'] < start_time) & (coor_dir['end_time'] > end_time))]
                # # print(match_coor)
                # # print(group_data_road)
                # road_dir = []
                # coor_dir_up = []
                # coor_dir_down = []
                # for indexs in match_coor.index:
                #     up_nodeid = match_coor.loc[indexs].values[2]
                #     down_nodeid = match_coor.loc[indexs].values[3]
                #     coor_rdsectid = match_coor.loc[indexs].values[3]
                #     for j in noid_inf:
                #         if up_nodeid == j[0]:
                #             coor_dir_up.append(j[1])
                #         if down_nodeid == j[0]:
                #             coor_dir_down.append(j[1])
                road_dir = []
                for i in range(len(up_node)):
                    forward = 1
                    backward = 0
                    coor_forward = 0
                    coor_backward = 0
                    for j in range(len(down_node)):
                        if up_node[i] == down_node[j] and up_node[j] == down_node[i]:
                            backward = -1
                    for m in range(len(coor_dir_up)):
                        if up_node[i] == coor_dir_up[m] and down_node[i] == coor_dir_down[m]:
                            coor_forward = 1
                        if down_node[i] == coor_dir_up[m] and up_node[i] == coor_dir_down[m]:
                            coor_backward = 1
                    road_dir.append([up_node[i], down_node[i], forward, backward, coor_forward, coor_backward])

                data = alarm_times_match(match_node, noid_inf, group_key_node, int_name, match_order)
                relation = deleta_excess(road_dir, noid_inf)
                data_type = "1"
                # 日期格式转换
                k = k.replace('_', '')
                sub_id = k
                var_data = {'relation': relation, 'data': data}

                var_data = json.dumps(var_data, ensure_ascii=False)
                # print(var_data)
                json_send(conn, data_type, sub_id, var_data, start_date, end_date, start_time, end_time)
                geom_json(conn, start_date, end_date, start_time, end_time, noid_inf)
                # print(var_data)
        else:
            geom_json(conn, start_date, end_date, start_time, end_time, [])
            print("无组团数据，退出程序")
            pass
        conn.close()


if __name__ == '__main__':
    # geom_json('2018-04-01', '2018-04-10', '07:00:00', '09:00:00')
    # call_rdsect_geom('2018-03-05', '2018-03-15', '07:00:00', '09:00:00')
    main('2018-05-28', '2018-05-28', '06:00:00', '06:15:00', [])
    # call_coor_dir('2018-03-05', '2018-03-15', '07:00:00', '09:00:00')
    # call_node_inf()
    # call_coor_dir()