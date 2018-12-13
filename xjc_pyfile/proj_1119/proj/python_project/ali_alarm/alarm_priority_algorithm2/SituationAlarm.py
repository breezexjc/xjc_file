#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
author: xjc
version: 20181023
project：报警分类及排序
"""
import psycopg2
# from .database import *
from .database import *
import pandas as pd
import numpy as np
import sys
import multiprocessing
import threading
# from multiprocessing import Process,Queue
import json
import time
import queue
from scipy.stats import norm
from sklearn.neighbors import KernelDensity
import re,os
import datetime as dt
import shelve
from contextlib import closing
import gc   # 垃圾回收
import dbm

# IF_TEXT = False
# TEXT_DATE = '2018-10-12 09:44:00'
####################################

INT_NUM = 15
TIME_POINT = 96
# GROUPCOLUMN = 'date'
GROUPCOLUMN = 'weekday'
DRAW_INTERVAL = 15
PLOT_DATA_SAVE = r"insert into kde_alarm_predict values(%s,%s,%s,%s,%s,%s)"
PLOT_DATA_SAVE2 = r'insert into disposal_alarm_kde_distribution values(%s,%s,%s,%s,%s,%s)'
KDE_RANGE = 0.5
KDE_TABLE = 'disposal_alarm_kde_distribution'
ALARM_DATA_TABLE = 'disposal_alarm_data'
KDE_RESULT_TABLE = 'disposal_alarm_data_kde_value'
IF_SCATS_ID = False
ALARM_PERCENT = 75
PG = Postgres()


class AlarmData():
    local_time = dt.datetime.now()
    today = local_time.date()
    yesterday = (local_time-dt.timedelta(days=1)).date()
    last_month = (local_time-dt.timedelta(days=30)).date()
    sql_get_kde_data = r'select * from {0} where time_point>{1}'.format(KDE_TABLE,str(last_month))
    sdate = edate = str(local_time.date())
    stime = '00:00:00'
    etime = str(local_time.time())
    # print(sdate, edate, stime, etime)
    sql_alarm_data = "select a.*,b.rdsectid ,(case when (a.inter_id=b.gaode_intid_down and to_char(a.f_angle,'999.9')" \
                " ~ b.f_angle) then b.down_node when (a.inter_id=b.gaode_intid_up and to_char(a.t_angle,'999.9')" \
                " ~ b.t_angle) then b.up_node end) as scats_id,(case when (a.inter_id=b.gaode_intid_down and " \
                "to_char(a.f_angle,'999.9') ~ b.f_angle) then  b.import_desc when (a.inter_id=b.gaode_intid_up " \
                "and to_char(a.t_angle,'999.9') ~ b.t_angle) then b.export_desc end) as dir_desc " \
                "from (select inter_id,inter_name,coors,time_point,t_angle,f_angle,delay " \
                "from {4} where  inter_id in (%s) and to_char(time_point,'yyyy-mm-dd') = '{0}' " \
                "and to_char(time_point,'hh24:mi:ss') between '{2}' and '{3}')a LEFT JOIN " \
                "gaode_alarm_rdsect_match b " \
                "on ((a.inter_id=b.gaode_intid_down and b.f_angle!='-1' and to_char(a.f_angle,'999.9') ~ b.f_angle) " \
                "or (a.inter_id=b.gaode_intid_up and b.t_angle!='-1' and to_char(a.t_angle,'999.9') ~ b.t_angle)) " \
                "order by a.inter_id,a.time_point;" \

    sql_alarm_data_init = "select a.*,b.rdsectid ,(case when (a.inter_id=b.gaode_intid_down and to_char(a.f_angle,'999.9')" \
                     " ~ b.f_angle) then b.down_node when (a.inter_id=b.gaode_intid_up and to_char(a.t_angle,'999.9')" \
                     " ~ b.t_angle) then b.up_node end) as scats_id,(case when (a.inter_id=b.gaode_intid_down and " \
                     "to_char(a.f_angle,'999.9') ~ b.f_angle) then  b.import_desc when (a.inter_id=b.gaode_intid_up " \
                     "and to_char(a.t_angle,'999.9') ~ b.t_angle) then b.export_desc end) as dir_desc " \
                     "from (select inter_id,inter_name,coors,time_point,t_angle,f_angle,delay " \
                     "from {0} where to_char(time_point,'yyyy-mm-dd') = '{1}' " \
                     ")a LEFT JOIN " \
                     "gaode_alarm_rdsect_match b " \
                     "on ((a.inter_id=b.gaode_intid_down and b.f_angle!='-1' and to_char(a.f_angle,'999.9') ~ b.f_angle) " \
                     "or (a.inter_id=b.gaode_intid_up and b.t_angle!='-1' and to_char(a.t_angle,'999.9') ~ b.t_angle)) " \
                     "where b.rdsectid is not null order by a.inter_id,a.time_point;" .format(ALARM_DATA_TABLE, sdate)

    sql_his_alarm_get = "select a.inter_id,a.inter_name,a.time_point,a.delay,b.rdsectid,(case when " \
                    "(a.inter_id=b.gaode_intid_down and to_char(a.f_angle,'999.9') ~ b.f_angle) " \
                    "then b.down_node when (a.inter_id=b.gaode_intid_up and to_char(a.t_angle,'999.9') " \
                    "~ b.t_angle) then b.up_node end) as scats_id, (case when (a.inter_id=b.gaode_intid_down " \
                    " and to_char(a.f_angle,'999.9') ~ b.f_angle) then b.import_desc when (a.inter_id=" \
                    "b.gaode_intid_up and to_char(a.t_angle,'999.9') ~ b.t_angle) then b.export_desc end )" \
                    "as dir_desc from( select inter_id,inter_name,time_point,t_angle,f_angle,delay " \
                    "from {0} where time_point between CURRENT_DATE::TIMESTAMP-'30 day'" \
                    "::INTERVAL and CURRENT_DATE )a LEFT JOIN gaode_alarm_rdsect_match b on (a.inter_id=" \
                    "b.gaode_intid_down and  b.f_angle!='-1' and to_char(a.f_angle,'999.9') ~ b.f_angle) or " \
                    "(a.inter_id=b.gaode_intid_up and b.t_angle!='-1' and to_char(a.t_angle,'999.9') ~ b.t_angle);" \
    .format(ALARM_DATA_TABLE)

    sql_kde_send = r"insert into disposal_alarm_kde_distribution values(%s,%s,%s,%s,%s,%s,%s)"
    sql_get_kde_his_data = r"select * from disposal_alarm_kde_statistic where cal_date='%s' and site_id in (%s) "
    sql_send_kde_result = r"insert into {0}(inter_id,time_point,new_kde_value,his_kde_value,alarm_type) " \
                          r"values(%s,%s,%s,%s,%s)".format(KDE_RESULT_TABLE)

    def __init__(self, inter_list = None, rdsectid=None):
        self.rdsectid = rdsectid
        self.sql_alarm_data = AlarmData.sql_alarm_data
        self.sql_kde_send = AlarmData.sql_kde_send
        self.inter_list = inter_list
        self.db = PG

    def call_pg_data(self, sql):
        result = self.db.call_pg_data(sql, fram=True)
        return result

    def send_pg_data(self, sql, data = None):
        self.db.send_pg_data(sql,data)


    def get_alarm_data_today(self):
        alarm_data = self.call_pg_data(self.sql_alarm_data_init)
        return alarm_data

    def get_new_alarm_data(self, inter_list):
        pram = ''
        for int in inter_list:
            pram = pram + '\'' + int + '\','
        local_time = dt.datetime.now()
        # local_time = dt.datetime.strptime('2018-10-09 17:00:00', '%Y-%m-%d %H:%M:%S')
        today = local_time.date()
        sdate = edate = str(local_time.date())
        stime = '00:00:00'
        etime = str(local_time.time())
        alarm_data = self.call_pg_data(self.sql_alarm_data.format(sdate, edate, stime, etime, ALARM_DATA_TABLE) % pram[:-1])
        # print(self.sql_alarm_data % pram[:-1])
        # print(alarm_data)
        return alarm_data


    def get_his_alarm_data(self):
        alarm_data = self.call_pg_data(AlarmData.sql_his_alarm_get)
        return alarm_data


    def kde_data_send(self,data):
        self.send_pg_data(self.sql_kde_send, data)

    def get_kde_his_data(self,scats_list):
        pram = ''
        for int in scats_list:
            pram = pram + '\'' + int + '\','
        kde_his_data = self.call_pg_data(self.sql_get_kde_his_data%(AlarmData.yesterday, pram[:-1]))
        # print(self.sql_get_kde_his_data%(AlarmData.yesterday, pram[:-1]))
        return kde_his_data

    def send_kde_result(self,data):
        self.db.send_pg_data(AlarmData.sql_send_kde_result, data)


            # sys.exit(0)
            # return pd.DataFrame({})
        # self.send_pg_data(AlarmData.sql_send_kde_result, data)


class KDE_filter():
    def __init__(self, date=None):
        # self.data = alarm_data
        self.date = date
        self.plot_save = None
        self.kde_model = {}

        # self.draw_plot(plot_data,X_plot, X_axis, date)

    def model_initialize(self):
        A = AlarmData()
        alarm_data = A.get_alarm_data_today()
        print("tody_alarm", alarm_data)
        try:
            self.kde_data_translate(alarm_data)
        except TypeError:
            print("匹配不到报警数据")
        else:
            self.kde_cal()

    def his_model_initialize(self):
        A = AlarmData()
        alarm_data = A.get_his_alarm_data()
        try:
            self.kde_data_translate(alarm_data)
        except TypeError:
            print("匹配不到报警数据")
        else:
            self.kde_cal()

    def kernel_predict(self, X):
        X_plot = self.X_plot
        for kernel in ['gaussian',
                       # 'tophat', 'epanechnikov'
                       ]:
            kde = KernelDensity(kernel=kernel, bandwidth=KDE_RANGE).fit(X)

            log_dens = kde.score_samples(X_plot)

        return kde,log_dens

    def kde_data_translate(self, data):
        # data = self.data
        data['date'] = data['time_point'].apply(lambda x: x.date() if x else None)
        data['time'] = data['time_point'].apply(lambda x: (x.hour * 60 + x.minute) / DRAW_INTERVAL if x else None)
        data['weekday'] = data['time_point'].apply(lambda x: x.weekday() + 1 if x else None)
        alarm_data = data
        if IF_SCATS_ID:
            grouped = alarm_data.groupby(['scats_id', 'weekday', 'dir_desc'])
        else:
            grouped = alarm_data.groupby(['inter_id', 'weekday', 'dir_desc'])
        plot_data = {}
        # print(grouped)
        # 数据分组-按日期分组绘图
        for (k1, k2, k3), group in grouped:
            # 历史数据
            if plot_data.get(k1):
                pass
            else:
                plot_data[k1] = {}
            if plot_data[k1].get(k2):
                pass
            else:
                plot_data[k1][k2] = {}
            # 历史数据
            X = group['time'][:, np.newaxis]
            resectid = list(set(group['rdsectid'].values))[0]
            grouped_day = group.groupby(['date']).size()
            alarm_num = grouped_day.values
            # np.median(alarm_num)  # 中位数
            percent_value = np.percentile(alarm_num, ALARM_PERCENT)  # 分位数
            # print('percent_value', percent_value)
            # print(k1, '路口', percent_value)
            # X = np.array(result)[:, np.newaxis]
            # group.groupby(['date']).sum()
            days = len(group['date'].unique())
            # 最新数据
            X_new = None
            X_time = group['time_point'][:, np.newaxis]
            int_name = list(set(list(group['inter_name'])))
            plot_data[k1][k2][k3] = [X, X_time, X_new, int_name[0], days, percent_value,resectid]
        # 初始化时间轴，日期随意
        datetime_start = dt.datetime.strptime("2018-09-26 00:00:00", "%Y-%m-%d %H:%M:%S")
        datatime_1day = dt.timedelta(hours=23.75)
        datetime_end = datetime_start + datatime_1day

        X_axis = pd.date_range(datetime_start, datetime_end, freq=str(DRAW_INTERVAL * 60) + 's')[:, np.newaxis]
        X_plot = np.linspace(0, int(24 * 60 / DRAW_INTERVAL) - 1, int(24 * 60 / DRAW_INTERVAL))[:, np.newaxis]
        self.plot_data = plot_data
        self.X_plot = X_plot
        self.X_axis = X_axis

    def kde_data_translate_weekday(self, data):
        # data = self.data
        data['date'] = data['time_point'].apply(lambda x: x.date() if x else None)
        data['time'] = data['time_point'].apply(lambda x: (x.hour * 60 + x.minute) / DRAW_INTERVAL if x else None)
        data['weekday'] = data['time_point'].apply(lambda x: x.weekday() + 1 if x else None)
        # 子分类 = GROUPCOLUMN
        # group_column = GROUPCOLUMN
        # data_select = data[data['inter_id']== '14LHD097JA0']
        # print(data_select)
        # 分方向区分
        # dir_alarm_regulartion(data,None)
        alarm_data = data
        if IF_SCATS_ID:
            grouped = alarm_data.groupby(['scats_id', 'weekday', 'dir_desc'])
        else:
            grouped = alarm_data.groupby(['inter_id', 'weekday', 'dir_desc'])
        plot_data = {}
        # print(grouped)
        # 数据分组-按日期分组绘图
        for (k1, k2, k3), group in grouped:
            # 历史数据
            if plot_data.get(k1):
                pass
            else:
                plot_data[k1] = {}
            if plot_data[k1].get(k2):
                pass
            else:
                plot_data[k1][k2] = {}
            # 历史数据
            X = group['time'][:, np.newaxis]
            # X = np.array(result)[:, np.newaxis]
            days = len(group['date'].unique())
            # 最新数据
            X_new = None
            X_time = group['time_point'][:, np.newaxis]
            int_name = list(set(list(group['inter_name'])))
            plot_data[k1][k2][k3] = [X, X_time, X_new, int_name[0], days]

        datetime_start = dt.datetime.strptime("2018-09-26 00:00:00", "%Y-%m-%d %H:%M:%S")
        datatime_1day = dt.timedelta(hours=23.75)
        datetime_end = datetime_start + datatime_1day
        X_axis = pd.date_range(datetime_start, datetime_end, freq=str(DRAW_INTERVAL * 60) + 's')[:, np.newaxis]
        X_plot = np.linspace(0, int(24 * 60 / DRAW_INTERVAL) - 1, int(24 * 60 / DRAW_INTERVAL))[:, np.newaxis]
        self.plot_data = plot_data
        self.X_plot = X_plot
        self.X_axis = X_axis

    def kde_cal(self):
        plot_data = self.plot_data
        X_plot = self.X_plot
        X_axis = self.X_axis
        int_list = list(plot_data.keys())
        plot_data_save = []
        alarm_plot_data_save = []
        for i in range(len(int_list)):
            # ax = plt.subplot(331)
            int_id = int_list[i]
            int_match_data = plot_data[int_list[i]]
            date_list = list(int_match_data.keys())
            # time_aixs = np.arange(0, int(24 * 60 / DRAW_INTERVAL), 12)
            # time_list = [str(i)[11:13] for i in X_axis[:, 0]]
            for index_date, value in enumerate(date_list):
                week_match_data = int_match_data[date_list[index_date]]
                date = value
                dir_desc = list(week_match_data.keys())
                for index_dir, value in enumerate(dir_desc):
                    dir = value
                    X = week_match_data[dir][0]
                    days = week_match_data[dir][4]
                    # print(np.percentile(a, 25))
                    avg_alarm = len(X) / days
                    percent_value = week_match_data[dir][5]
                    # print(percent_value)
                    resectid = week_match_data[dir][6]
                    int_name = week_match_data[dir][3]
                    if len(X) > 0:
                        kde_model, log_dens_his = self.kernel_predict(X)
                        self.kde_model[int_id+ '-'+dir+'-'+str(date)] = [kde_model, percent_value,resectid]
                        # print(kde_model)
                        # log_dens_his_data = list(avg_alarm * np.exp(log_dens_his))
                    else:
                        pass
                        # log_dens_his = None
                        # log_dens_his_data = None
                    # 计算核密度分布
                    # current_date = dt.datetime.now().date()
                    # plot_data_save.append(
                    #     [int_list[i], int_name, current_date, dir, list(X_plot[:, 0]), log_dens_his_data])
        # return plot_data_save
        # resolve_kde_data = []
        resolve_plot_data = []
        # for data in plot_data_save:
        #     site_id = data[0]
        #     int_name = data[1]
        #     date = data[2]
        #     dir_desc = data[3]
        #     time_label = data[4]
        #     kde_alarm_his = data[5]
        #     # kde_alarm_new = data[5]
        #
        #     def check_kde(list_kde, num):
        #         if list_kde[num] <= 0.01:
        #             q_kde_alarm = 0.00
        #         else:
        #             q_kde_alarm = round(list_kde[num], 2)
        #         return q_kde_alarm
        #
        #     for num in range(len(time_label)):
        #         time_stamp = time_label[num] * DRAW_INTERVAL
        #         hour = int(math.floor(time_stamp / 60))
        #         min = int(math.floor(time_stamp % 60))
        #         # date = dt.datetime.now().date()
        #         q_time = dt.time(hour, min, 0, 0)
        #         q_datetime = dt.datetime.strptime(str(date) + ' ' + str(q_time), '%Y-%m-%d %H:%M:%S')
        #
        #         # print(q_datetime)
        #         q_kde_alarm_his = check_kde(kde_alarm_his, num)
        #         # q_kde_alarm_new = check_kde(kde_alarm_new, num)
        #         # check_regular_alarm(q_kde_alarm_his, q_kde_alarm_new)
        #         weekday = q_datetime.weekday()
        #         resolve_kde_data.append([site_id, int_name, dir_desc, weekday, time_stamp, q_kde_alarm_his, str(date)])
        #         resolve_plot_data.append([site_id, int_name, dir_desc, weekday, q_time, q_kde_alarm_his, str(date)])
        #         # time_stamp = dt.datetime.fromtimestamp(time_stamp)
        #         # if q_kde_alarm_his > 0 or q_kde_alarm_new >0:
        #         #     print([site_id, int_name, week_day, q_time, q_kde_alarm_his, q_kde_alarm_new])
        # return resolve_kde_data
        # alarm_plot_data_save(resolve_plot_data, PLOT_DATA_SAVE2, KDE_STATISTIC)

    def get_kde_his_data(self):


        pass


class CheckAlarm(object):
    sql_check_alarm = "SELECT a.inter_id,a.inter_name,d.inter_id as node_id,d.systemid as scats_id,a.count " \
                      "as alarm_count " \
                      "from(select inter_id,inter_name ,count(*) from {0} where time_point " \
                      "BETWEEN to_timestamp(to_char(current_date,'yyyy-MM-dd')||' 00:00:00','yyyy-MM-dd hh24:mi:ss') " \
                      "and to_timestamp(to_char(current_date,'yyyy-MM-dd')||' 23:59:59','yyyy-MM-dd hh24:mi:ss') " \
                      "GROUP BY inter_id,inter_name " \
                      ")a LEFT JOIN (select b.*,c.systemid from gaode_inter_rel b,pe_tobj_node c " \
                      "where b.inter_id = c.nodeid)d on a.inter_id = d.gaode_id;".format(ALARM_DATA_TABLE)

    sql_new_alarm_time ="select a.inter_id,a.inter_name,a.time_point,a.delay,b.rdsectid, (case when " \
                        "(a.inter_id=b.gaode_intid_down and to_char(a.f_angle,'999.9') ~ b.f_angle) then b.down_node " \
                        "when (a.inter_id=b.gaode_intid_up and to_char(a.t_angle,'999.9') ~ b.t_angle) " \
                        "then b.up_node end) as scats_id,(case when (a.inter_id=b.gaode_intid_down and to_" \
                        "char(a.f_angle,'999.9') ~ b.f_angle) then b.import_desc when (a.inter_id=b.gaode_intid_up " \
                        "and to_char(a.t_angle,'999.9') ~ b.t_angle) then b.export_desc end )as dir_desc " \
                        "from( select inter_id,inter_name,time_point,t_angle,f_angle,delay from {0} " \
                        "where time_point= (select max(time_point) as new_alarm_time from {0}) )a " \
                        "LEFT JOIN gaode_alarm_rdsect_match b on (a.inter_id=b.gaode_intid_down and  b.f_angle!='-1' " \
                        "and b.down_node is not null " \
                        "and to_char(a.f_angle,'999.9') ~ b.f_angle) or (a.inter_id=b.gaode_intid_up and b.t_angle!='-1'" \
                        " and b.up_node is not null " \
                        "and to_char(a.t_angle,'999.9') ~ b.t_angle) ".format(ALARM_DATA_TABLE)
    # print(sql_new_alarm_time)
    def __init__(self):
        self.last_alarm_time = None
        self.new_alarm = None

    def clear_kde_table(self):
        current_date = (dt.datetime.now()-dt.timedelta(days=1)).date()
        sql_delete = "delete from  {0} where time_piint<'{1}'".format(KDE_RESULT_TABLE, current_date)
        db = PG
        conn, cr = db.db_conn()
        if conn is not None:
            try:
                cr.execute(sql_delete)
            except Exception as e:
                conn.commit()
                print(e)
            else:
                conn.commit()
                print("his data clear success!")

    def call_pg_data(self, sql):
        result = PG.call_pg_data(sql,fram=True)
        return result

    def count_alarm_data(self):
        self.new_alarm = self.call_pg_data(CheckAlarm.sql_check_alarm)
        if self.last_count:
            alarm_filter()
        else:
            self.last_count = self.new_count

    def kde_value_search(self,kde_alarm,time_serice):
        grouped = kde_alarm.groupby(['site_id', 'weekday', 'dir_desc'])
        kde_alarm = []
        for (int,week,dir),group in grouped:
            for i in range(len(group)):
                scats_id = group.iloc[i][1]
                kde_time_serice = group.iloc[i][4]
                kde_value = group.iloc[i][5]
                try:
                    next_kde_time_serice = group.iloc[i + 1][4]
                    next_kde_value = group.iloc[i + 1][5]
                except IndexError:
                    continue
                if kde_time_serice < time_serice and next_kde_time_serice > time_serice:
                    kde_alarm_new = kde_value-(kde_value-next_kde_value)*(time_serice-kde_time_serice)/DRAW_INTERVAL
                    if kde_alarm_new > 0:
                        kde_alarm.append([int, week, dir,time_serice, kde_alarm_new])
                else:
                    pass

        # for index, kde_date in enumerate(kde_alarm):
        #     scats_id = kde_date[1]
        #     kde_time_serice = kde_date[4]
        #     kde_value = kde_date[5]
        #     next_kde_time_serice = kde_alarm[index + 1][4]
        #     next_kde_value = kde_alarm[index + 1][5]
        #     if kde_time_serice < time_serice and next_kde_time_serice > time_serice:
        #         kde_alarm_new = kde_value-(kde_value-next_kde_value)*(time_serice-kde_time_serice)/DRAW_INTERVAL
        #
        #         return kde_alarm_new
        #     else:
        #         pass
        return kde_alarm

    def match_his_kde(self, kde_his_data, time_serice):
        grouped = kde_his_data.groupby(['site_id', 'weekday', 'dir_desc'])
        kde_alarm = []
        for (int, week, dir), group in grouped:
            for i in range(len(group)):
                scats_id = group.iloc[i][0]
                kde_time = group.iloc[i][3]
                kde_time_serice = kde_time.hour*60+kde_time.minute
                kde_value = group.iloc[i][4]
                try:
                    next_kde_time = group.iloc[i + 1][3]
                    next_kde_time_serice = next_kde_time.hour*60+next_kde_time.minute
                    next_kde_value = group.iloc[i + 1][4]
                except IndexError:
                    continue
                if kde_time_serice < time_serice and next_kde_time_serice > time_serice:
                    kde_alarm_new = kde_value - (kde_value - next_kde_value) * (
                                time_serice - kde_time_serice) / DRAW_INTERVAL
                    kde_alarm.append([int, week, dir, time_serice, kde_alarm_new])
                else:
                    pass
        return kde_alarm

    def get_new_alarm_inf(self):
        delay_dict = {}
        self.new_alarm = self.call_pg_data(CheckAlarm.sql_new_alarm_time)
        self.new_alarm_time = self.new_alarm['time_point'].drop_duplicates().values
        # print(type(self.new_alarm_time[0]))
        # print('new alarm data \n',self.new_alarm)
        time_point = pd.to_datetime(self.new_alarm_time[0]).to_pydatetime()
        inter_list = list(self.new_alarm['inter_id'].drop_duplicates().values)
        site_id_list = list(self.new_alarm['scats_id'].drop_duplicates().values)
        for i in range(len(self.new_alarm)):
            scats_id = self.new_alarm.iloc[i][5]
            dir_desc = self.new_alarm.iloc[i][6]
            delay = self.new_alarm.iloc[i][3]
            if scats_id is not None:
                delay_dict[scats_id+'-'+dir_desc] = delay
            else:
                continue

        return inter_list, site_id_list, time_point, delay_dict

    def new_alarm_match(self, K, q):
        # inter_list, site_id_list, time_point = self.check_new_alarm()
        try:
            inter_list, site_id_list, time_point, delay_dict = self.get_new_alarm_inf()
        except TypeError as e:
            pass
        else:
            if self.last_alarm_time:
                if self.last_alarm_time == time_point:
                    print("无报警数据更新")
                    return None
                else:
                    start_time = dt.datetime.now()
                    print(start_time, 'get new alarm data! starting kde value calculate...')
                    print(time_point)
                    kde_model = alarm_filter(K, inter_list, site_id_list)
                    q.put([time_point, kde_model, delay_dict])
                    local_time = dt.datetime.now()
                    print('cost_time:',(local_time-start_time).seconds, 's #kde value calculate finished!')
                    # kde_new_alarm = alarm_filter(inter_list)
                    self.last_alarm_time = time_point
                    # time_serice = time_point.hour*60+time_point.minute
                    return [time_point, kde_model]

            else:
                self.last_alarm_time = time_point
                local_time = dt.datetime.now()
                print(local_time, 'get new alarm data! starting kde value calculate...')
                # time_serice = (time_point.hour * 60 + time_point.minute) / DRAW_INTERVAL
                kde_model = alarm_filter(K, inter_list, site_id_list)
                q.put([time_point, kde_model,delay_dict])
                local_time = dt.datetime.now()
                print(local_time, 'kde value calculate finished!')
                return [time_point, kde_model]



    def his_alarm_match(self,K):
        kde_model = alarm_filter(K)
        return kde_model
        # new_kde_result = self.new_kde_predict(kde_model, time_serice)
        # print(kde_model)
        # queue.put(new_kde_result)


class KdeModelSave():
    def __init__(self):
        self.model_file_dir = r'.\kde_model'
        self.mkdir(self.model_file_dir)

    def mkdir(self, path):
        folder = os.path.exists(path)
        if not folder:  # 判断是否存在文件夹如果不存在则创建为文件夹
            os.makedirs(path)  # makedirs 创建文件时如果路径不存在会创建这个路径
            print("---  new folder...  ---")
            print("---  OK  ---")
        else:
            print("---  There is this folder!  ---")

    def save_model(self,model_file,file_name):
        with closing(shelve.open(self.model_file_dir+'\\' + file_name, 'c')) as f:
            f['model'] = model_file


    def read_model(self,file_name):
        with closing(shelve.open(self.model_file_dir + '\\' + file_name, 'r')) as f:
            model_file = f['model']
        return model_file

    def remove_model(self, file_name):
        try:
            os.remove(self.model_file_dir+'\\'+file_name)
        except Exception as e:
            print('remove_model', e)


def alarm_filter(K, inter_list=None, site_id_list=None):

    A1 = AlarmData()
    if inter_list is not None and site_id_list is not None:
        match_data = A1.get_new_alarm_data(inter_list)
        if len(match_data) > 0:
            K.kde_data_translate(match_data)
            try:
                kde_data = K.kde_cal()
            except Exception as e:
                print(e)
            kde_model = K.kde_model
            # kde_his_data = A1.get_kde_his_data(site_id_list)
            # print(kde_his_data)
            # print(kde_data)
            # A1.kde_data_send(kde_data)
            # return kde_data, kde_his_data
            return kde_model
        else:
            print("can't get alarm data on %s" % dt.datetime.now().date())
            return None
    else:
        match_data = A1.get_his_alarm_data()
        if len(match_data) > 0:
            K.kde_data_translate(match_data)
            kde_data = K.kde_cal()
            kde_model = K.kde_model
            # print(kde_data)
            # A1.kde_data_send(kde_data)
            # return kde_data, kde_his_data
            return kde_model



