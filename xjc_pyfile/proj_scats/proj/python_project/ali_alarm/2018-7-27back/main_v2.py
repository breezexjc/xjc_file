#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import Spatial_Association_Analysis

import GlobalContent
import psycopg2
import pandas as pd
import time
import datetime as dt
import tkinter as tk
from tkinter.simpledialog import askstring, askinteger, askfloat

# 生成相位车道匹配数据
def create_phase_match():
    try:
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
    except Exception as e:
        print('链接数据库失败', e)
    else:
        cr = conn.cursor()
        try:
            cr.callproc(GlobalContent.al_phase_match_pro, [])
        except Exception as e:
            print("create_phase_match:", e)
        finally:
            conn.close()
    return


# 实时运行
def main(isworkday):

    # 获取系统时间，确定初始计算日期
    local_time = dt.datetime.now()
    date_day = local_time.date()
    date_time = local_time.time()
    # 生成计算时间序列，以5分钟为间隔
    time_period = pd.date_range(str(date_day) + " 00:00:00", str(date_day) + " 23:59:59", freq='5min')
    # 转换成datetime格式
    time_period = time_period.to_pydatetime()
    time_list = [i for i in time_period]
    print(time_list)
    while True:
        local_time = dt.datetime.now()
        print(str(local_time)[0:19])
        dt_local_time = dt.datetime.strptime(str(local_time)[0:19] ,"%Y-%m-%d %H:%M:%S")
        now_day = local_time.date()
        # 判断是否过了一天
        if now_day == date_day:
            pass
        else:
            # 更新日期
            date_day = now_day
            time_period = pd.date_range(str(date_day) + " 00:00:00", str(date_day) + " 23:59:59", freq='5min')
            time_period = time_period.to_pydatetime()
            time_list = [i for i in time_period]
            print('开始新的一天,重新创建计算时间序列')
        # 循环计算各时间段内组团数据
        for i in range(len(time_list)):
            if time_list[i] + dt.timedelta(minutes=5) > dt_local_time > time_list[i]:
                end_time = str(time_list[i])[11:]
                dt_end_time = dt.datetime.strptime(end_time, '%H:%M:%S')-dt.timedelta(minutes=6)
                start_time = dt.datetime.strftime(dt_end_time, '%H:%M:%S')
                print([start_time, end_time])
                # 开始拥堵空间关联分析
                Spatial_Association_Analysis.main(str(date_day), str(date_day), start_time, end_time, isworkday)
                # 计算完成，删除当前计算时间
                del time_list[i]
                break
            else:
                pass
        if len(time_list) == 0:
            end_time = "23:59:59"
            dt_end_time = dt.datetime.strptime(end_time, '%H:%M:%S')-dt.timedelta(minutes=6)
            start_time = dt.datetime.strftime(dt_end_time, '%H:%M:%S')
            Spatial_Association_Analysis.main(str(date_day), str(date_day), start_time, end_time, isworkday)
        time.sleep(60)


if __name__ == '__main__':
    # local_time = dt.datetime.now()
    # date_day = local_time.date()
    # # cal_date = date_day
    # cal_date = '2018-05-28'
    # isworkday = 2
    # # 创建计算时间区段
    # time_period = pd.date_range(cal_date + " 06:00:00", cal_date + " 23:59:59", freq='5min')
    # print(time_period)
    # cal_time = [['07:00:00', '08:00:00'], ['08:00:00', '09:00:00'], ['16:00:00', '17:00:00'], ['17:00:00', '18:00:00']]

    print("Calculation finished")

