#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
author: xjc
version: 20181015
project：报警分类及排序
"""

# import numpy as np
# import sys
# from log_record import LogRecord
# from proj.proj.config.database import *
# from .SituationTraffic import *
# from .SituationOperate import *
from ..alarm_priority_algorithm3.SituationAlarm import *
# from SituationTraffic import *
# from SituationOperate import *
# from SituationAlarm import *
# from database import *
import threading
# from multiprocessing import Process,Queue
import time
import queue
import datetime as dt
import gc  # 垃圾回收
import dbm
from .control_pram import IF_TEST
import logging

LogAlarm = logging.getLogger('sourceDns.webdns.views')  # 获取settings.py配置文件中logger名称
# Log = LogRecord()
# date = dt.datetime.now().date()
# LogAlarm = Log.create_log("ali_alarm_proj_"+str(date))

# 图表显示中文
####################################

TEXT_DATE = '2018-10-20 23:54:00'
TEST_WEEK = 3
####################################
# from ali_alarm.log_record import LogRecord

INT_NUM = 15
TIME_POINT = 96
# GROUPCOLUMN = 'date'
# GROUPCOLUMN = 'weekday'
# DRAW_INTERVAL = 15
PLOT_DATA_SAVE = r"insert into kde_alarm_predict values(%s,%s,%s,%s,%s,%s)"
PLOT_DATA_SAVE2 = r'insert into disposal_alarm_kde_distribution values(%s,%s,%s,%s,%s,%s)'
# KDE_RANGE = 1.0
KDE_TABLE = 'disposal_alarm_kde_distribution'
ALARM_DATA_TABLE = 'disposal_alarm_data'
KDE_RESULT_TABLE = 'disposal_alarm_data_kde_value'
IF_SCATS_ID = False
# ALARM_PERCENT = 90
# SO_INTERVAL = '15minutes'


def alarm_filter(K, inter_list=None, site_id_list=None):
    A1 = AlarmData()
    if inter_list is not None and site_id_list is not None:
        match_data = A1.get_new_alarm_data(inter_list)
        if len(match_data) > 0:
            K.kde_data_translate(match_data)
            kde_data = K.kde_cal()
            kde_model = K.kde_model
            # kde_his_data = A1.get_kde_his_data(site_id_list)
            # print(kde_his_data)
            # print(kde_data)
            # A1.kde_data_send(kde_data)
            # return kde_data, kde_his_data
            return kde_model
        else:
            LogAlarm.warning("can't get alarm data on %s" % dt.datetime.now().date())
            # print("can't get alarm data on %s" % dt.datetime.now().date())
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


def new_alarm_get(q):
    # global Log
    local_time = dt.datetime.now()
    current_date = local_time.date()
    # print(local_time, "start")
    try:
        # K_HIS = KDE_filter()
        # K_HIS.his_model_initialize()  # 历史KDE模型初始化
        # model_save = KdeModelSave()
        # model_save.save_model(K_HIS.kde_model, 'KdeHisModel_%s' % str(current_date))    # 保存模型到本本地
        start_date = current_date
        C2 = CheckAlarm()
        K1 = KDE_filter()
        K1.model_initialize()  # 当日KDE模型初始化
        # start_date = dt.datetime.now().date()
        C1 = CheckAlarm()
    except Exception as e:
        LogAlarm.info("报警KDE计算程序初始化失败！")
        LogAlarm.error(e)
    else:
        LogAlarm.info("报警KDE计算程序初始化成功！")
        while True:
            local_time = dt.datetime.now()
            current_date = local_time.date()
            LogAlarm.info('checking new alarm data ... ')
            # print(local_time, 'checking new alarm data ... ')
            try:
                if current_date != start_date:  # 凌晨执行历史KDE模型更新
                    # Log.create_log(filename='log_%s' % current_date)
                    _unreachable = gc.collect()
                    # print("unreachable object num:%d" % (_unreachable))
                    # print("garbage object num:%d" % (len(gc.garbage)))
                    try:
                        # his_model = C2.his_alarm_match(K_HIS)
                        # model_save.save_model(his_model, 'KdeHisModel_%s' % str(current_date))
                        C1.clear_kde_table()  # 清空前天历史数据
                        model_new = C1.new_alarm_match(K1, q)  # 计算新报警数据
                        start_date = current_date
                    except Exception as e:
                        LogAlarm.error(e)

                else:
                    try:
                        DB = C1.new_alarm_match(K1, q)
                        LogAlarm.info('kde value calculate finished!')
                    except Exception as e:
                        LogAlarm.error(e)

                    time.sleep(60)
            except Exception as e:
                LogAlarm.error(e)
                time.sleep(60)


# 更新历史模型
def his_model_update():
    local_time = dt.datetime.now()
    last_date = local_time - dt.timedelta(days=1)
    current_date = local_time.date()
    # print(local_time, "start")
    try:
        K_HIS = KDE_filter()
        K_HIS.his_model_initialize()  # 历史KDE模型初始化
        model_save = KdeModelSave()
        model_save.save_model(K_HIS.kde_model, 'KdeHisModel_%s' % str(current_date))  # 保存模型到本本地
        # C1 = CheckAlarm()
        # C1.clear_kde_table()  # 清空前天历史数据
    except Exception as e:
        LogAlarm.info("报警KDE计算程序初始化失败！")
        LogAlarm.error(e)
    else:
        try:
            os.remove(r'.\kde_model' + r'\KdeHisModel_%s.bak' % str(last_date.date()))
            os.remove(r'.\kde_model' + r'\KdeHisModel_%s.dat' % str(last_date.date()))
            os.remove(r'.\kde_model' + r'\KdeHisModel_%s.dir' % str(last_date.date()))
        except Exception as e:
            print('历史模型删除失败', e)
        print("报警KDE历史模型计算成功！")
        LogAlarm.info("报警KDE历史模型计算成功！")


def kde_result_resolve_real(data, time_point):
    result = []
    for (key, value) in data.items():
        key_split = key.split('-')
        int_id = key_split[0]
        dir = key_split[1]
        weekday = key_split[2]
        new_kde_value = value.get('KdeValue')
        his_kde_value = value.get('HisKdeValue')
        if new_kde_value > his_kde_value:
            alarm_type = 1
        else:
            alarm_type = 0
        # time_point = value.get('TimePoint')
        result.append([int_id, dir, weekday, time_point, new_kde_value, his_kde_value, alarm_type])
    df_kde_result = pd.DataFrame(result, columns=['int_id', 'dir', 'weekday', 'time_point', 'new_kde_value',
                                                  'his_kde_value', 'alarm_type'])
    df_int_sum = df_kde_result.groupby(['int_id', 'weekday', 'time_point'])[
        ['new_kde_value', 'his_kde_value', 'alarm_type']].sum()
    # for i in range(len(df_int_sum)):
    #     if df_int_sum.iloc[i][]
    # print(df_int_sum.agg(['sum']))
    # print(df_int_sum)
    result = []

    for index, value in df_int_sum.iterrows():
        if value[2] > 1:
            value[2] = 1
        result.append([index[0], index[2], value[0], value[1], str(value[2])])

    # df_int_sum_values = df_int_sum.values
    return result


def kde_data_send(df_kde_result):
    try:
        A = AlarmData()
        message = A.send_kde_result(df_kde_result)
        LogAlarm.info(message)
    except Exception as e:
        LogAlarm.error(e)


def kde_predict(kde_model, time_point):
    key_list = kde_model.keys()
    # print("历史模型路口", key_list)
    time_array = np.array([time_point])[:, np.newaxis]
    kde_result = {}
    local_time = dt.datetime.now()
    if IF_TEST:
        local_weekday = TEST_WEEK
    else:
        local_weekday = local_time.weekday() + 1
    year, month, day = (local_time.year, local_time.month, local_time.day)

    for key in key_list:
        model_weekday = key.split('-')[2]
        # print("weekday", local_weekday, model_weekday)
        if str(local_weekday) == model_weekday:
            model = kde_model.get(key)[0]
            alarm_count = kde_model.get(key)[1]
            rdsectid = kde_model.get(key)[2]
            kde_value = model.score_samples(time_array)
            hour = time_array[:, 0] * DRAW_INTERVAL / 60
            minute = time_array[:, 0] * DRAW_INTERVAL % 60
            alarm_time = dt.datetime(year, month, day, hour, minute, 0)
            if IF_TEST:
                str_alarm_time = TEXT_DATE
            else:
                str_alarm_time = dt.datetime.strftime(alarm_time, '%Y-%m-%d %H:%M:%S')
            fre = list(alarm_count * np.exp(kde_value))[0]
            if fre < 0.1:  # 小于0.1的报警强度强制为0
                fre = 0.0
            # print(int_id, dir_desc, round(fre, 2))
            kde_result[key] = {'KdeValue': round(fre, 2), 'TimePoint': str_alarm_time, 'RdsectId': rdsectid}
        else:
            pass
    return kde_result


# 处理接口请求
def new_kde_cal():
    # global Log
    local_time = dt.datetime.now()
    current_date = local_time.date()
    LogAlarm.info('checking new alarm data ... ')
    # print(local_time, 'checking new alarm data ... ')
    K1 = KDE_filter()
    C1 = CheckAlarm()
    try:
        DB = C1.new_alarm_match_real(K1)
        LogAlarm.info('kde value calculate finished!')
    except Exception as e:
        LogAlarm.error(e)
        time.sleep(60)
    else:
        if DB:
            time_point = DB[0]
            kde_model = DB[1]
            model_save = KdeModelSave()
            # print("# get KdeHisModel succeed!")
            LogAlarm.info("# get KdeHisModel succeed!")
            try:
                his_kde_model = model_save.read_model('KdeHisModel_%s' % str(current_date))
            except FileNotFoundError as e:
                print("# get KdeHisModel failed!")
                # LogAlarm.info(e)

                return
            except dbm.error as e:
                print("# get KdeHisModel failed!")
                LogAlarm.info("kde model file not found")

                return
            else:
                print("# get KdeHisModel succeed!")
                date_list = time_point
                date_serice = (time_point.hour * 60 + time_point.minute) / DRAW_INTERVAL
                new_kde_model = kde_model
                # new_alarm_int = new_kde_model.keys()
                # key_list = his_kde_model.keys()
                his_kde_model_match = {}
                # 匹配报警路口历史模型

                for key, value in new_kde_model.items():
                    result = his_kde_model.get(key)
                    if result is not None:
                        his_kde_model_match[key] = result
                try:
                    his_kde_result = kde_predict(his_kde_model_match, date_serice)
                    new_kde_result = kde_predict(new_kde_model, date_serice)
                    if his_kde_result:
                        for key in new_kde_result.keys():
                            his_value = his_kde_result.get(key)
                            if his_value:
                                new_kde_result[key]['HisKdeValue'] = his_value.get('KdeValue')
                            else:
                                new_kde_result[key]['HisKdeValue'] = 0
                    df_kde_result = kde_result_resolve_real(new_kde_result, date_list)
                    return df_kde_result
                except Exception as e:
                    print(e)
                    LogAlarm.error("异常错误")
                    return
        else:
            print("数据库连接失败！")
            pass


# def his_alarm_kde_cal(q):
#     current_date = dt.datetime.now().date()
#     K_HIS = KDE_filter()
#     K_HIS.his_model_initialize()    # 历史KDE模型初始化
#     # q.put(K_HIS.kde_model)
#     model_save = KdeModelSave()
#     model_save.save_model(K_HIS.kde_model, 'KdeHisModel_%s'%str(current_date))
#     start_date = dt.datetime.now().date()
#     C1 = CheckAlarm()
#     while True:
#         current_date = dt.datetime.now().date()
#         if current_date > start_date:   # 凌晨执行历史KDE模型更新
#             _unreachable = gc.collect()
#             # print("unreachable object num:%d" % (_unreachable))
#             # print("garbage object num:%d" % (len(gc.garbage)))
#             start_date = current_date
#             his_model = C1.his_alarm_match(K_HIS)
#             model_save.save_model(his_model, 'KdeHisModel_%s' % str(current_date))
#             # print(his_model)
#             # q.put(his_model)
#         else:
#             time.sleep(15)


def alarm_type_judge(q1):
    global SA

    def kde_predict(kde_model, time_point):
        key_list = kde_model.keys()
        time_array = np.array([time_point])[:, np.newaxis]
        kde_result = {}
        local_time = dt.datetime.now()
        local_weekday = local_time.weekday() + 1
        year, month, day = (local_time.year, local_time.month, local_time.day)

        for key in key_list:
            model_weekday = key.split('-')[2]
            # print("weekday", local_weekday, model_weekday)
            if str(local_weekday) == model_weekday:
                model = kde_model.get(key)[0]
                alarm_count = kde_model.get(key)[1]
                rdsectid = kde_model.get(key)[2]
                kde_value = model.score_samples(time_array)
                hour = time_array[:, 0] * DRAW_INTERVAL / 60
                minute = time_array[:, 0] * DRAW_INTERVAL % 60
                alarm_time = dt.datetime(year, month, day, hour, minute, 0)
                if IF_TEST:
                    str_alarm_time = TEXT_DATE
                else:
                    str_alarm_time = dt.datetime.strftime(alarm_time, '%Y-%m-%d %H:%M:%S')
                fre = list(alarm_count * np.exp(kde_value))[0]
                if fre < 0.1:  # 小于0.1的报警强度强制为0
                    fre = 0.0
                # print(int_id, dir_desc, round(fre, 2))
                kde_result[key] = {'KdeValue': round(fre, 2), 'TimePoint': str_alarm_time, 'RdsectId': rdsectid}
            else:
                pass
        return kde_result

    def kde_predict_test(kde_model, time_point):
        key_list = kde_model.keys()
        time_array = np.array([time_point])[:, np.newaxis]
        kde_result = {}
        local_time = dt.datetime.now()
        local_weekday = local_time.weekday() + 1
        year, month, day = (local_time.year, local_time.month, local_time.day)

        for key in key_list:
            model_weekday = key.split('-')[2]
            # print("weekday", local_weekday, model_weekday)
            # input()

            model = kde_model.get(key)[0]
            alarm_count = kde_model.get(key)[1]
            kde_value = model.score_samples(time_array)
            hour = time_array[:, 0] * DRAW_INTERVAL / 60
            minute = time_array[:, 0] * DRAW_INTERVAL % 60
            alarm_time = dt.datetime(year, month, day, hour, minute, 0)
            str_alarm_time = dt.datetime.strftime(alarm_time, '%Y-%m-%d %H:%M:%S')
            fre = list(alarm_count * np.exp(kde_value))[0]
            # print(int_id, dir_desc, round(fre, 2))
            kde_result[key] = {'KdeValue': round(fre, 2), 'TimePoint': str_alarm_time}

        return kde_result

    def kde_result_resolve(data, time_point):
        result = []
        for (key, value) in data.items():
            key_split = key.split('-')
            int_id = key_split[0]
            dir = key_split[1]
            weekday = key_split[2]
            new_kde_value = value.get('NewKdeValue')
            his_kde_value = value.get('KdeValue')
            if new_kde_value > his_kde_value:
                alarm_type = 1
            else:
                alarm_type = 0
            # time_point = value.get('TimePoint')
            result.append([int_id, dir, weekday, time_point, new_kde_value, his_kde_value, alarm_type])
        df_kde_result = pd.DataFrame(result, columns=['int_id', 'dir', 'weekday', 'time_point', 'new_kde_value',
                                                      'his_kde_value', 'alarm_type'])
        df_int_sum = df_kde_result.groupby(['int_id', 'weekday', 'time_point'])[
            ['new_kde_value', 'his_kde_value', 'alarm_type']].sum()
        # for i in range(len(df_int_sum)):
        #     if df_int_sum.iloc[i][]
        # print(df_int_sum.agg(['sum']))
        # print(df_int_sum)
        result = []

        for index, value in df_int_sum.iterrows():
            if value[2] > 1:
                value[2] = 1
            result.append([index[0], index[2], value[0], value[1], str(value[2])])

        # df_int_sum_values = df_int_sum.values
        return result

    def kde_data_send(df_kde_result):
        try:

            A = AlarmData()
            message = A.send_kde_result(df_kde_result)
            LogAlarm.info(message)
        except Exception as e:
            LogAlarm.error(e)

    def situation_of_alarm_cal(his_kde_result, delay_cict):
        alarm_keys = delay_cict.keys()
        SA_Value = {}
        for (key, value) in his_kde_result.items():
            key_split = key.split('-')
            int_id = key_split[0]
            dir = key_split[1]
            weekday = key_split[2]
            new_kde_value = value.get('NewKdeValue')
            rdsectid = value.get('RdsectId')
            his_kde_value = value.get('KdeValue')
            if int_id + '-' + dir in alarm_keys:
                delay = delay_cict.get(int_id + '-' + dir)
                SA_Value[int_id + '-' + dir] = {'RdsectId': rdsectid, 'SAValue': delay * new_kde_value}
        return SA_Value

    start_date = dt.datetime.now().date()
    model_save = KdeModelSave()
    while True:
        try:
            his_kde_model = model_save.read_model('KdeHisModel_%s' % str(start_date))
        except FileNotFoundError as e:
            LogAlarm.info(e)
            time.sleep(60)
        except dbm.error as e:
            LogAlarm.info("kde model file not found", e)
            time.sleep(60)
        else:
            LogAlarm.info("# get KdeHisModel succeed!")
            while True:
                current_date = dt.datetime.now().date()
                if current_date > start_date:
                    start_date = current_date
                    break
                model_new = q1.get(True)
                time_point = model_new[0]
                delay_cict = model_new[2]

                if IF_TEST:
                    date_list = pd.date_range('2018-10-20 00:00:00', '2018-10-20 23:00:00', freq='2min')
                    date_serice = [(i.hour * 60 + i.minute) / DRAW_INTERVAL for i in date_list]
                else:
                    date_list = [time_point]
                    date_serice = [(time_point.hour * 60 + time_point.minute) / DRAW_INTERVAL]
                new_kde_model = model_new[1]
                try:
                    for i in range(len(date_serice)):
                        his_kde_result = kde_predict(his_kde_model, date_serice[i])
                        new_kde_result = kde_predict(new_kde_model, date_serice[i])
                        if his_kde_result:
                            # new_kde_result = kde_predict_test(new_kde_model, time_serice)
                            for key in his_kde_result.keys():
                                new_value = new_kde_result.get(key)
                                # his_value = his_kde_result.get(key)
                                if new_value:
                                    # delta = new_value.get('KdeValue')-his_value.get('KdeValue')
                                    his_kde_result[key]['NewKdeValue'] = new_value.get('KdeValue')
                                else:

                                    his_kde_result[key]['NewKdeValue'] = 0
                                    # new_kde_result[key]['AlarmType'] = 2    # 未知类型
                        df_kde_result = kde_result_resolve(his_kde_result, date_list[i])
                        kde_data_send(df_kde_result)
                        SA = situation_of_alarm_cal(his_kde_result, delay_cict)
                except Exception as e:
                    LogAlarm.error(e)


def create_process():
    global SA, SO, ST

    SA = pd.DataFrame({})
    SO = pd.DataFrame({})
    ST = pd.DataFrame({})
    #####################多线程############################
    all_thread = {}
    q1 = queue.Queue()
    t1 = threading.Thread(target=new_alarm_get, args=(q1,), name='KDE模型计算')
    t2 = threading.Thread(target=alarm_type_judge, args=(q1,), name='报警类型分析')
    # t3 = threading.Thread(target=so_run, args=(), name='匹配操作记录')
    # all_thread['t1'] = ([t1,new_alarm_get, (q1,), t1.name])
    # all_thread['t2'] = ([t2,alarm_type_judge, (q1,), t2.name])
    t1.daemon = True  # 1、必须在p.start()之前
    t2.daemon = True
    # t3.daemon = True
    try:
        t1.start()
        # t3.start()
        t2.start()
    except Exception as e:
        LogAlarm.error(e)
    else:
        LogAlarm.info("报警数据引擎启动！")
    # check_threading(all_thread)
    # try:
    #     t3.join()
    # except:
    #     print('Caught an exception')


def check_threading(thread_list):
    while True:
        for key in thread_list.keys():
            value = thread_list.get(key)
            thread = value[0]
            target = value[1]
            args = value[2]
            name = value[3]
            alive = thread.is_alive()

            if alive:
                pass
            else:
                t = threading.Thread(target=target, args=args, name=name)
                t.start()
            # print(threading.enumerate())
        time.sleep(60)


# def so_run():
#     interval = SO_INTERVAL
#     while True:
#         local_time = dt.datetime.now()
#         stime = local_time-dt.timedelta(minutes=10)
#         etime = local_time
#         stime = dt.datetime.strftime(stime, '%Y-%m-%d %H:%M:%S')
#         etime = dt.datetime.strftime(etime, '%Y-%m-%d %H:%M:%S')
#         stime = '2018-10-22 00:00:00'
#         etime = '2018-10-22 23:00:00'
#         args = [stime, etime, interval]
#         S1 = SituationOperate()
#         try:
#             S1.call_pg_function(S1.sql_alarm_operate_match, args)
#         except Exception as e:
#             LogAlarm.error('so_run',e)
#         else:
#             LogAlarm.info("完成一轮调控记录匹配")
#         finally:
#             pass
#             # print("完成一轮调控记录匹配")
#         time.sleep(60)


if __name__ == '__main__':
    new_kde_cal()
    # S1 = SituationOperate()
    # result = S1.operate_statistic()

    # if result is not None:
    #     SO = S1.data_solve()
    # else:
    #     SO = None
    # T = TrafficSituation()
    # ST = T.get_salklist()
