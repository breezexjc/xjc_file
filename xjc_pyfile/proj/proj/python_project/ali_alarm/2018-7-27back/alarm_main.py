#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
sys.path.insert(0, 'H:\program\\ali_alarm')
import Spatial_Association_Analysis
import Spatial_Association_Analysisv2
import GlobalContent
import psycopg2
import pandas as pd
import time
import datetime as dt
import tkinter as tk
import time
from tkinter.simpledialog import askstring, askinteger, askfloat
import sys, time
import psycopg2 as pg


class CONTENT():
    time_interval = True
    if time_interval:
        time_delay = 5
    else:
        time_delay = 30

class ShowProcess():
    """
    显示处理进度的类
    调用该类相关函数即可实现处理进度的显示
    """
    i = 0 # 当前的处理进度
    max_steps = 0 # 总共需要处理的次数
    max_arrow = 50 #进度条的长度
    infoDone = 'done'

    # 初始化函数，需要知道总共的处理次数
    def __init__(self, max_steps, infoDone = 'Done'):
        self.max_steps = max_steps
        self.i = 0
        self.infoDone = infoDone

    # 显示函数，根据当前的处理进度i显示进度
    # 效果为[>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>]100.00%
    def show_process(self, i=None):
        if i is not None:
            self.i = i
        else:
            self.i += 1
        num_arrow = int(self.i * self.max_arrow / self.max_steps) #计算显示多少个'>'
        num_line = self.max_arrow - num_arrow #计算显示多少个'-'
        percent = self.i * 100.0 / self.max_steps #计算完成进度，格式为xx.xx%
        process_bar = '[' + '>' * num_arrow + '-' * num_line + ']'\
                      + '%.2f' % percent + '%' + '\r' #带输出的字符串，'\r'表示不换行回到最左边
        print(process_bar)
        # 控制台显示
        sys.stdout.write(process_bar) #这两句打印字符到终端
        sys.stdout.flush()
        if self.i >= self.max_steps:
            self.close()

    def close(self):
        print('')
        print(self.infoDone)
        self.i = 0




# 接收一个整数
# def print_integer():
#     res = askinteger("Spam", "Egg count", initialvalue=12*12)
#     print(res)
#     return res
#     # 接收一个浮点数
# def print_float():
#     res = askfloat("Spam", "Egg weight\n(in tons)", minvalue=1, maxvalue=100)
#     print(res)
#     return res
#     # 接收一个字符串
# def print_string():
#     res = askstring("Spam", "Egg label")
#     print(res)
#     return res

# root = tk.Tk()
# r1 = tk.Button(root, text='取一个字符串', command=print_string).pack()
# r2 = tk.Button(root, text='取一个字符串', command=print_string).pack()
# r3 = tk.Button(root, text='取一个字符串', command=print_string).pack()
# Spatial_Association_Analysis.main(r1, r1, r2, r3, 2)
# root.mainloop()

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


def warper(func):
    def count_time():
        time_start = time.time()
        func()
        time_end = time.time()
        print('totally cost', time_end - time_start)
    return count_time


def alarm_group(cal_date_start, cal_time, isworkday):

    # local_time = dt.datetime.now()
    # date_day = local_time.date()
    # time_period = pd.date_range(cal_date + " 06:00:00", cal_date + " 23:59:59", freq='5min')
    # 循环计算各区段内组团数据

    for i in range(len(cal_time)):
        for j in range(len(cal_time[i])):
            end_time = str(cal_time[i][j])[11:]
            dt_end_time = dt.datetime.strptime(end_time, '%H:%M:%S')-dt.timedelta(minutes=CONTENT.time_delay)
            start_time = dt.datetime.strftime(dt_end_time, '%H:%M:%S')
            print([start_time, end_time])

            # 拥堵空间关联分析
            try:
                Spatial_Association_Analysisv2.main(cal_date_start, start_time, end_time, isworkday)
            except Exception as e:
                print(e)
    # end_time = "23:59:59"
    # dt_end_time = dt.datetime.strptime(end_time, '%H:%M:%S')-dt.timedelta(minutes=15)
    # start_time = dt.datetime.strftime(dt_end_time, '%H:%M:%S')
    # Spatial_Association_Analysis.main(cal_date_start, cal_date_end, start_time, end_time, isworkday)
    return


def real_main(isworkday):
    err_inf2 = []
    now = dt.datetime.now()
    start_date = ('%s-%s-%s' % (now.year, now.month, now.day))
    # start_date = '2017-12-22'
    one_day = dt.timedelta(hours=24)  # 获取一天的时间间隔
    datetime_start = dt.datetime.strptime(start_date + ' 00:00:00', '%Y-%m-%d %H:%M:%S')  # 今日起始时间
    datetime_end = datetime_start + one_day  # 今日结束时间
    interval = 900
    current_date = start_date
    time_interval = dt.timedelta(seconds=900)
    time_list = pd.date_range(datetime_start, datetime_end, freq='%ss' % interval)  # 创建时间序列
    time_list = time_list.tolist()
    while True:

        local_time = dt.datetime.now()  # 获取当前时间
        current_date = ('%s-%s-%s' % (local_time.year, local_time.month, local_time.day))
        if current_date == start_date:  # 判断是否经过了一天
            # print(time_list)
            for i in range(len(time_list) - 1):  # 匹配当前时间，确实数据计算起始时间cal_start和结束时间cal_end
                # print(str(time_list[i]))
                if dt.datetime.strptime(str(time_list[i]), '%Y-%m-%d %H:%M:%S') < local_time < \
                        dt.datetime.strptime(str(time_list[i + 1]), '%Y-%m-%d %H:%M:%S'):
                    print(local_time, "start")
                    cal_start = time_list[i] - time_interval
                    cal_end = time_list[i]
                    start = dt.datetime.strftime(cal_start, '%Y-%m-%d %H:%M:%S')  # datetime转化成字符串
                    end = dt.datetime.strftime(cal_end, '%Y-%m-%d %H:%M:%S')
                    date_star = start[0:10]
                    time_star = start[11:19]
                    date_end = end[0:10]
                    time_end = end[11:19]
                    try:
                        Spatial_Association_Analysis.main(date_star, date_end, time_star, time_end, isworkday)
                    except Exception as e:
                        err_inf2.append({"real_main-Spatial_Association_Analysis：",e})
                    else:
                        err_inf2.append({"real_main-Spatial_Association_Analysis：", '正常运行'})
                elif dt.datetime.strptime(str(time_list[i]), '%Y-%m-%d %H:%M:%S') > local_time:
                    sleep_time = 30
                    time.sleep(sleep_time)
                    print('sleep %ss' % sleep_time)
                    break
                else:
                    continue
        else:
            datetime_start = dt.datetime.strptime(current_date + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
            datetime_end = datetime_start + one_day
            time_list = pd.date_range(datetime_start, datetime_end, freq='%ss' % interval)
            time_list = time_list.tolist()
            start_date = current_date
        time.sleep(30)

        if len(err_inf2) > 0:
            print(err_inf2)
            try:
                f = open('G:\PROGRAME\log\\real_alarm' + str(current_date) + '.txt', 'r+')
                f.read()
            except FileNotFoundError:
                f = open('G:\PROGRAME\log\\real_alarm' + str(current_date) + '.txt', 'w')

            f.write('runRealTimeNetCap' + '\n')
            f.write(str(err_inf2) + '\n')
            f.write(str(dt.datetime.now()) + '结束' + '\n')
            print("实时报警组团模块日志记录完毕")
            f.close()
            err_inf2 = []


@warper
def main():
    try:
        conn = psycopg2.connect(database=GlobalContent.pg_database72_research['database'],
                                user=GlobalContent.pg_database72_research['user'],
                                password=GlobalContent.pg_database72_research['password'],
                                host=GlobalContent.pg_database72_research['host'],
                                port=GlobalContent.pg_database72_research['port'])
    except Exception as e:
        print(e)
    else:
        cr_pg = conn.cursor()
        local_time = dt.datetime.now()
        date_day = local_time.date()
        cal_date = date_day
        # cal_date = input("请输入开始日期")
        # end_date = input("请输入结束日期")
        cal_date = '2017-11-01'
        end_date = '2017-12-01'
                   # '2018-06-01'
        date_period = pd.date_range(cal_date, end_date, freq='1440min')
        isworkday = 2
        max_steps = len(date_period)
        process_bar = ShowProcess(max_steps, 'OK')
        sql_delete = "delete from alarm_transfer_json_data where start_date = '{0}'"
        sql_delete2 = "delete from alarm_group where alarm_date = '{0}'"
        sql_delete3 = "delete from alarm_group_rdsectid where alarm_date = '{0}'"
        sql_delete4 = "delete from alarm_rdsect_match_result where date_day = '{0}'"
        # 创建计算时间区段
        for i in date_period:
            json_date = str(str(i)[0:10].replace('-', ''))
            cr_pg.execute(sql_delete.format(json_date))
            cr_pg.execute(sql_delete2.format(str(i)[0:10]+'~'+str(i)[0:10]))
            cr_pg.execute(sql_delete3.format(str(i)[0:10] + '~' + str(i)[0:10]))
            cr_pg.execute(sql_delete4.format(str(i)[0:10] + '~' + str(i)[0:10]))
            conn.commit()

            process_bar.show_process()
            time.sleep(0.01)
            # print(str(i)[0:10])
            # print(CONTENT.time_interval)
            time_period = []
            if CONTENT.time_interval:
                time_period1 = pd.date_range(str(i)[0:10] + " 07:00:00", str(i)[0:10] + " 10:00:00", freq='2min')
                time_period2 = pd.date_range(str(i)[0:10] + " 16:00:00", str(i)[0:10] + " 20:00:00", freq='2min')
                time_period = [time_period1, time_period2]
                # print(time_period)
                alarm_group(str(i)[0:10], time_period, isworkday)
            else:
                time_period1 = pd.date_range(str(i)[0:10] + " 07:00:00", str(i)[0:10] + " 09:30:00", freq='30min')
                time_period2 = pd.date_range(str(i)[0:10] + " 16:00:00", str(i)[0:10] + " 19:30:00", freq='30min')
                time_period = [time_period1, time_period2]
                # print(time_period)
                alarm_group(str(i)[0:10],  time_period, isworkday)
            # time_period2 = pd.date_range(str(i)[0:10] + " 07:00:00", str(i)[0:10] + " 9:30:00", freq='30min')
            # time_period = time_period1+time_period2
            # cal_time = [['07:00:00', '08:00:00'], ['08:00:00', '09:00:00'], ['16:00:00', '17:00:00'], ['17:00:00', '18:00:00']]
        cr_pg.close()
        conn.close()
        print("Calculation finished")
        return "100%"


if __name__ == '__main__':
    main()


