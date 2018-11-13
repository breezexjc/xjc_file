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

# 存放表名等信息
class CONTENT:
    alarm_group = 'alarm_group'  # 以节点形式存放组团划分结果
    alarm_match_data = 'alarm_rdsect_match_result'  # 报警数据匹配结果
    alarm_rdsect = 'alarm_rdsect'    # 加工后的路段信息
    alarm_group_rdsectid = 'alarm_group_rdsectid'   # 以路段形式存放组团划分结果
    alarm_subset = 'alarm_sub_group'    # 子团信息
    alarm_keynode = 'alarm_keynode'    # 关键节点
    alarm_node_suggest = 'alarm_node_suggest'   # 节点策略


# 获取报警数据
def call_postgres(conn, date_interval, time_interval):
    match_roadsect_result = pd.DataFrame({})
    match_roadsect_info = pd.DataFrame({})
    cr = conn.cursor()
    # 路段，上下游节点，进出口道信息
    # 筛选关联报警数据
    sql1 = "select rdsectid, up_to_down_match_num, date_day, down_to_up_match_num,time_interval, " \
          "forward_avg_delay,backward_avg_delay, geom, rdsect_desc, length from " + CONTENT.alarm_match_data + \
           " where  rdsectid != 'UTRSS001116' and date_day ='{0}' " \
           "and time_interval ='{1}' and " \
           "length<=800  and (up_to_down_match_num>= 1 OR down_to_up_match_num>= 1) order by rdsectid"\
           .format(date_interval, time_interval)
    cr.execute(sql1)
    rs1 = cr.fetchall()
    match_roadsect_result = pd.DataFrame(rs1)
    match_roadsect_result.columns = ['rdsectid', 'up_to_down_match_num', 'date_day', 'down_to_up_match_num', 'time_interval',
                                     'forward_avg_delay', 'backward_avg_delay', 'geom', 'rdsect_desc', 'length']

    sql2 = "select * from " + CONTENT.alarm_rdsect
    cr.execute(sql2)
    rs1 = cr.fetchall()
    match_roadsect_info = pd.DataFrame(rs1)
    match_roadsect_info.columns = ['rdsectid', 'up_node', 'down_node', 'road_dir', 'dir_desc',
                                   'import_desc', 'export_desc', 'fstr_desc', 'length']
    cr.close()
    return match_roadsect_result, match_roadsect_info


# 组团生成
def group_create(merge_result):
    down = []
    up = []
    all_node = []
    rdsect = []

    for m in range(len(merge_result)):
        down.append(merge_result.iloc[m]['down_node'])
        up.append(merge_result.iloc[m]['up_node'])
        rdsect.append(merge_result.iloc[m]['rdsectid'])
    all_node.extend(down)
    all_node.extend(up)
    result = []
    all_node = list(set(all_node))
    up_no_repeat = list(set(up))
    root = [down[0]]
    group_add(root, down, up)



# 根据车流交汇来判断关键节点
def key_node(group_result, merge_result, max_alarm_data):
    # print(group_result)
    for i in group_result:
        max_node = []
        change_list = []
        group_id = list(i.keys())
        index = max_alarm_data.index

        for m in range(len(max_alarm_data)):
            # print(max_alarm_data)
            if list(index)[m] == group_id[0] and max_alarm_data:
                max_node.append(max_alarm_data.iloc[m])

        # print(i.values())
        node_list = list(i.values())[0]
        key_node_list = []
        subset = []

        for j in node_list:
            all_node = []
            match_data = merge_result[(merge_result['down_node'] == j) | (merge_result['up_node'] == j)]
            # print(match_data)
            all_node.extend(np.array(match_data['down_node']).tolist())
            all_node.extend(np.array(match_data['up_node']).tolist())
            distinct_node = set(all_node)
            up_match_data = merge_result[(merge_result['up_node'] == j)]
            down_match_data = merge_result[(merge_result['down_node'] == j)]
            up_node_num = len(up_match_data)
            down_node_num = len(down_match_data)
            if up_node_num + down_node_num > 2:
                key_node_list.append(j)
                key_subset = {'KeyNode': j, 'all_node': distinct_node, 'Subset': np.array(match_data['rdsectid']).tolist()}
                subset.append(key_subset)
            elif up_node_num + down_node_num == 1:
                pass
            elif up_node_num + down_node_num == 2:
                key_node_list.append(j)
                key_subset = {'KeyNode': j, 'all_node': distinct_node, 'Subset': np.array(match_data['rdsectid']).tolist()}
                subset.append(key_subset)
            else:
                pass
            change_list.extend(np.array(match_data['rdsectid']).tolist())
        rdsect_set.append({str(group_id[0]): [list(set(change_list)), key_node_list, max_node, subset]})

    print(rdsect_set)


# 报警数最多点判断
def max_alarm_node(group_data):
    # print(group_data)
    group_group_data = group_data.groupby(['group_id']).apply(lambda t: t[t.alarm_times==t.alarm_times.max()])    # 分组
    # 求每组报警次数最大值
    # print(group_group_data)

    # if group_group_data
    #     # print(group_group_data['alarm_times'].idxmax())
    #     max_alarm = group_group_data.apply(lambda frame: frame['scats_id'][frame['alarm_times'].idxmax()])
    #     print(max_alarm)
    # else:
    #     max_alarm = []
    return group_group_data


# 组团，关键节点数据存入数据库
def insert_postgres(group_result, date_interval, time_interval):
    try:
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'], user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'], host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
    except Exception as e:
        print(e)
    else:

        cr = conn.cursor()
        # 发送路段形式报警组团
        sql_delete = "delete from {0} where  alarm_date ='{1}' and time_point='{2}' " \
            .format(CONTENT.alarm_group_rdsectid, date_interval, time_interval)
        try:
            cr.execute(sql_delete)
        except Exception as e:

            print('insert_postgres_sql_delete', e)
        # print(rdsect_set)
        for i in rdsect_set:
            group_id = list(i.keys())[0]
            rdsectid_set = list(i.values())[0][0]
            key_node = list(i.values())[0][1]
            max_node = list(i.values())[0][2]
            # print(rdsectid_set)

            # print(max_node)
            for j in rdsectid_set:
                sql = "insert into {0}(group_id, alarm_date, time_point, rdsectid) values(%s,%s,%s,%s)" \
                    .format(CONTENT.alarm_group_rdsectid)
                sql_delete = "delete from {0} where group_id='{1}' and alarm_date ='{2}' and time_point='{3}' and rdsectid ='{4}'" \
                    .format(CONTENT.alarm_group_rdsectid, group_id, date_interval, time_interval, j)
                try:
                    cr.execute(sql, (group_id, date_interval, time_interval, j))
                    conn.commit()
                except psycopg2.IntegrityError:
                    print('组团路段数据已存在')
            conn.commit()



        # 发送关键节点数据
        sql_delete = "delete from {0} where alarm_date ='{1}' and time_point = '{2}'"\
                .format(CONTENT.alarm_keynode, date_interval, time_interval)
        try:
            cr.execute(sql_delete)
        except Exception as e:
            print('insert_postgres_sql_delete',e)
        for i in rdsect_set:
            rdsectid_set = list(i.values())[0][0]
            key_node = list(i.values())[0][1]
            max_node = list(i.values())[0][2]
            group_id = list(i.keys())[0]

            key_node_result = [group_id, key_node, max_node]
            sql = "insert into {0}(group_id,key_node,alarm_date,time_point) values (%s,%s,%s,%s)".format(CONTENT.alarm_keynode)
            try:
                for m in range(len(key_node)):
                    cr.execute(sql, (group_id, key_node[m],date_interval, time_interval))
                    conn.commit()
            except psycopg2.IntegrityError as e:
                conn.commit()
                print('关键点已存在')
            try:
                cr.execute(sql, (group_id, max_node[0], date_interval, time_interval))
                conn.commit()
            except Exception as e:
                conn.commit()
                print(e, '关键点已存在')
        conn.close()


    return 'end'


# 组团数据存入数据库
def insert_alarm_group(group_result, date_interval, time_interval):
    try:
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
        cr = conn.cursor()
    except Exception as e:
        pass
    else:
        # 路段，上下游节点，进出口道信息
        # 发送节点形式报警组团
        sql_delete = "delete from " + CONTENT.alarm_group + " where alarm_date = '{0}' and time_point = '{1}'" \
                     .format(date_interval, time_interval)
        try:
            cr.execute(sql_delete)
        except Exception as e:
            print('insert_alarm_group_sql_delete',e)

        for i in group_result:
            group_id = list(i.keys())
            scats_id = list(i.values())
            for j in range(len(scats_id[0])):
                sql_delete = "delete from " + CONTENT.alarm_group + " where alarm_date = '{0}' and time_point = '{1}'" \
                                                                    " and group_id = '{2}' and scats_id = '{3}'" \
                    .format(date_interval, time_interval, group_id[0], scats_id[0][j])
                sql_insert = "insert into " + CONTENT.alarm_group + "(group_id,scats_id,alarm_date,time_point)" \
                                                                    " values (%s,%s,%s,%s)"
                try:
                    cr.execute(sql_insert, (group_id[0], scats_id[0][j], date_interval, time_interval))
                    conn.commit()
                except psycopg2.IntegrityError:
                    print('组团数据已存在')
                    conn.commit()
                conn.commit()
        conn.close()


# 获取组团数据
def call_group_data(conn, date_interval, time_interval):
    # conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'], user=GlobalContent.pg_database72_research['user'],
    #                         password=GlobalContent.pg_database72_research['password'], host=GlobalContent.pg_database72_research['host'],
    #                         port=GlobalContent.pg_database72_research['port'])
    cr = conn.cursor()
    sql = "select group_id,scats_id,alarm_times from {0} where alarm_date = '{1}' " \
          "and time_point= '{2}'".format(CONTENT.alarm_group, date_interval, time_interval)
    cr.execute(sql)
    rows = cr.fetchall()
    group_data = pd.DataFrame(rows)
    group_data.columns = ['group_id', 'scats_id', 'alarm_times']
    print(group_data)
    cr.close()
    return group_data


# 获取路段形式的报警组团数据
def call_alarm_group_rdsectid(date_interval, time_interval):
    conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'], user=GlobalContent.pg_database72_research['user'],
                            password=GlobalContent.pg_database72_research['password'], host=GlobalContent.pg_database72_research['host'],
                            port=GlobalContent.pg_database72_research['port'])
    cr = conn.cursor()

    sql = "select a.group_id, a.rdsectid, a.up_node, a.down_node, a.road_dir , b.dir_desc" \
          " from {0} a,{3} b where a.rdsectid = b.rdsectid and a.alarm_date ='{1}' " \
          "and a.time_point = '{2}'"\
        .format(CONTENT.alarm_group_rdsectid, date_interval, time_interval, CONTENT.alarm_group_rdsectid)
    cr.execute(sql)
    alarm_group_rdsectid = cr.fetchall()
    alarm_group_rdsectid.columns = ['group_id', 'rdsectid', 'up_node', 'down_node', 'road_dir', 'dir_desc']
    conn.close()
    return alarm_group_rdsectid


# 比较两个列表，找出不同元素
def compare_list(list1, list2):
    result = []
    for i in list2:
        if i not in list1:
            result.append(i)

    return result


# 递归搜索组团
def group_add(root, down, up):
    global rdsect_list
    global group_result
    global k
    node_match = []

    m = 0
    for i in root:  # 遍历根节点
        if i not in rdsect_list:
            rdsect_list.append(i)
        match_node = []
        # print('down', len(down))
        for j in range(len(down)):  # 找到上下游节点包含根节点的路段
            if down[j] == i:
                m += 1
                match_node.append(j)    # 找出符合条件的路段上游节点
                node_match.append(up[j])
                if up[j] not in rdsect_list:
                    rdsect_list.append(up[j])

            elif up[j] == i:
                m += 1
                match_node.append(j)    # 找出符合条件的路段下游节点
                node_match.append(down[j])
                if down[j] not in rdsect_list:
                    rdsect_list.append(down[j])
            else:
                pass
        if len(match_node) > 0:
            for n in match_node:
                up[n] = '-1'    # 已经匹配过的节点置为-1
                down[n] = '-1'
    # print(node_match)
    # print('m = ', m)
    # print(up)
    # print('k = ', k)
    if m == 0:
        k += 1
        num1 = 0
        num2 = 0
        rest_down = down
        rest_up = up
        for l in range(len(down)):
            if down[l] == '-1':
                num1 += 1

        for n in range(num1):
            rest_down.remove('-1')

        for l in range(len(up)):
            if up[l] == '-1':
                num2 += 1
        for n in range(num2):
            rest_up.remove('-1')

        # print('up_rest',len(rest_up))
        # print('down_rest', len(rest_down))
        if len(rest_down) > 0:

            root = [rest_down[0]]
            group_result.append({'group_'+str(k): rdsect_list})
            rdsect_list = []
            group_add(root, down, up)
        elif len(rest_up) > 0:

            root = [rest_up[0]]
            group_result.append({'group_'+str(k): rdsect_list})
            rdsect_list = []
            group_add(root, down, up)
        else:
            group_result.append({'group_' + str(k): rdsect_list})
            return 'over'
    else:
        root = node_match   # 将匹配到的节点定义为新的根节点
        group_add(root, down, up)


# 节点策略
def node_suggest():
    conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'], user=GlobalContent.pg_database72_research['user'],
                            password=GlobalContent.pg_database72_research['password'], host=GlobalContent.pg_database72_research['host'],
                            port=GlobalContent.pg_database72_research['port'])
    cr = conn.cursor()
    sql = "SELECT a.up_node,b.dir_desc ,-1 as suggest1, a.down_node,b.dir_desc,1 as suggest2 " \
          "FROM alarm_group_rdsectid_copy a,alarm_rdsect b where a.rdsectid=b.rdsectid"
    cr.execute(sql)
    suggest = cr.fetchall()
    node = []
    suggest_sign = []
    sql2 = "insert into {0} values(%s,%s,%s)".format(CONTENT.alarm_node_suggest)
    for i in range(len(suggest)):
        node.append(suggest[i][0])
        node.append(suggest[i][3])
        suggest_sign.append([suggest[i][1], suggest[i][2]])
        suggest_sign.append([suggest[i][4], suggest[i][5]])
        cr.execute(sql2, (suggest[i][0], suggest[i][1], suggest[i][2]))
        conn.commit()
        cr.execute(sql2, (suggest[i][3], suggest[i][4], suggest[i][5]))
        conn.commit()

    conn.close()


# 主程序
def main(start_date, start_time, end_time):
    global rdsect_list
    global rdsect_set
    global group_result
    global k
    date_interval = start_date
    time_interval = start_time + '~' + end_time
    rdsect_set = []
    group_result = []
    rdsect_list = []
    k = 0
    try:
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
    except Exception as e:
        print("数据库连接：", e)
    else:
        try:
            match_roadsect_result, match_roadsect_info = call_postgres(conn, date_interval, time_interval)
            # print(match_roadsect_result)
        except Exception as e:
            pass
            # print("match_roadsect_result无报警数据", e)
        else:
            # print("match_roadsect_info:",match_roadsect_info)
            # print("match_roadsect_result:", match_roadsect_result)

            df1 = pd.DataFrame({'rdsectid': match_roadsect_result['rdsectid']})
            df2 = pd.DataFrame({'rdsectid': match_roadsect_info['rdsectid'], 'up_node': match_roadsect_info['up_node'],
                                'down_node': match_roadsect_info['down_node']})
            merge_result = pd.merge(df1, df2, how='left', on='rdsectid')
            # print("merge_result:",merge_result)

        # print(merge_result)
            try:
                # 生成组团
                # create_phase_match(conn)
                group_create(merge_result)
                # insert_alarm_group(group_result, date_interval, time_interval)
            except Exception as e:
                print('group_create', e)
            else:
                try:
                    # 发送组团数据
                    # print(group_result)

                    insert_alarm_group(group_result, date_interval, time_interval)
                except Exception as e:
                    print('insert_alarm_group', e)
                else:
                    try:
                        # 获取组团信息及附加信息
                        group_data = call_group_data(conn, date_interval, time_interval)
                    except Exception as e:
                        print('group_data', e)
                    else:
                        # try:
                        #     # 关键点判断
                            max_alarm_data = max_alarm_node(group_data)
                            # print(max_alarm_data)
                            key_node(group_result, merge_result, max_alarm_data)
                        # except Exception as e:
                        #     print('max_alarm_data', e)
                        # else:
                            try:
                                # 发送路段形式组团数据和关键点数据
                                insert_postgres(group_result, date_interval, time_interval)
                            except Exception as e:
                                print('insert_postgres', e)
                            else:
                                keyword_match(date_interval)

        conn.close()


def keyword_match(date_interval):
    sql = "insert into alarm_group_keyword select  c.alarm_date,c.time_point,c.group_id,string_agg(c.road, ',') as keyroad  " \
          "FROM( select distinct a.alarm_date,a.time_point,a.group_id,split_part(b.fstr_desc,'（',1) as road " \
          "from alarm_group_rdsectid a,alarm_rdsect b " \
          "where a.rdsectid=b.rdsectid and a.alarm_date='{0}') c group by c.alarm_date,c.time_point,c.group_id " \
          "order by c.alarm_date,time_point,group_id"
    sql_delete = "delete from alarm_group_keyword where  alarm_date='{0}' "
    try:
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
    except Exception as e:
        print(e)
    else:
        cr = conn.cursor()
        cr.execute(sql_delete.format(date_interval))
        cr.execute(sql.format(date_interval))
        conn.commit()
        conn.close()



if __name__ == '__main__':
    # node_suggest()
    main('2017-11-01', '16:41:00', '16:46:00')

global rdsect_list
global rdsect_set
global group_result
global k
