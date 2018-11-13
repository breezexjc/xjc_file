#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pg数据库连接信息

# 72 public
# pg_database72 = {}
# pg_database72['database'] = "superpower"
# pg_database72['user'] = "dataer"
# pg_database72['password'] = "postgres"
# pg_database72['host'] = "192.168.22.72"
# pg_database72['port'] = "5432"

# 72 research
pg_database72_research= {}
pg_database72_research['database'] = "research"
pg_database72_research['user'] = "dataer"
pg_database72_research['password'] = "postgres"
pg_database72_research['host'] = "192.168.22.72"
pg_database72_research['port'] = "5432"

# local
# pg_database = {}
# pg_database['database'] = "postgis"
# pg_database['user'] = "postgres"
# pg_database['password'] = "postgres"
# pg_database['host'] = "192.168.22.232"
# pg_database['port'] = "5432"


alarm_match_data = 'alarm_rdsect_match_result'  # 关联报警匹配完成结果
alarm_record = 'alarm_record'   # 原始报警数据
alarm_rdsect = 'alarm_rdsect'   # 加工后的路段信息
alarm_group = 'alarm_group'  # 以节点形式存放组团划分结果
alarm_group_rdsectid = 'alarm_group_rdsectid'  # 以路段形式存放组团划分结果
alarm_subset = 'alarm_sub_group'  # 子团信息
alarm_keynode = 'alarm_keynode'  # 关键节点
alarm_node_suggest = 'alarm_node_suggest'  # 节点策略
alarm_holidays = 'alarm_holidays'
alarm_phase_match = 'alarm_phase_match'
alarm_transfer_json_data = 'alarm_transfer_json_data'



flow_combine = [['1'], ['2'], ['3']]
workdays = [0, 1, 2, 3]

