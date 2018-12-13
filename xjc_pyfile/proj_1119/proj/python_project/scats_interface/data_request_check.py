"""
检查数据勤请求状况，晚上指定时间对数据异常时段进行二次请求
作者：xjc
2018-12-12
"""
from .pram_conf import *
import cx_Oracle
import numpy as np
import datetime as dt
from .runStrategicinfo_getdata import int_grouped, RequestDynaDataFromInt
from .Request_Data_From_Int import RequestDynaDataFromInt as operate_request
from proj.tools.func_timer import timer

from proj.config.database import Postgres
from proj.config.sql_text import SqlText
import threading


class InterfaceCheck():

    def __init__(self):
        self.interface_list = interface_list
        self.scats_id_list = []
        self.int_str_input = []
        self.int_group = 0
        self.scats_id_init()  # 初始化路口编号、战略输入信息（配置表）

    def scats_id_init(self):
        try:
            conn = cx_Oracle.connect(OracleUser)  # 连接数据库
        except Exception as e:
            print('MainProcess:连接数据库失败', e)
        else:
            cr = conn.cursor()  # 建立游标
            try:
                cr.execute("SELECT * FROM INT_STR_INPUT order by SITEID")  # 从Oracle中读取数据
                IntStrInput = cr.fetchall()
                cr.execute(" select SITEID from INTERSECT_INFORMATION order by SITEID ")
                IntersectIDlist = cr.fetchall()

            except Exception as e:
                print("oracle路口信息数据获取失败", e)
            else:
                cr.close()
                conn.commit()
                conn.close()
                int_id = [i[0] for i in IntersectIDlist]
                int_num = len(int_id)
                print("请求总路口数：", int_num)
                group = round(int_num / CONSTANT.group_interval, 0) + 1
                int_grouped_data = int_grouped(int_id, group)
                self.int_group = int(group)
                self.scats_id_list = int_grouped_data
                self.int_str_input = IntStrInput

    @timer
    def salk_list_request(self):
        CONSTANT.IF_DATA_REPAIR = True
        print("开始数据修复")
        yesterday = (dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y-%m-%d') + ' 00:00:00'
        today = (dt.datetime.now()).strftime('%Y-%m-%d') + ' 00:00:00'
        loss_time = self.loss_data_period_check('战略运行记录接口', yesterday, today)
        print('loss_time:%s' % loss_time)
        # thread_creat(self.int_group, self.scats_id_list, self.int_str_input)
        if loss_time is not None:
            for time in loss_time:
                CONSTANT.S_REPAIR_DATE = dt.datetime.strftime(dt.datetime.strptime(time, '%Y-%m-%d %H:%M:%S') -
                                                              dt.timedelta(minutes=15), '%Y-%m-%d %H:%M:%S')
                CONSTANT.E_REPAIR_DATE = time
                threads = []
                for i in range(self.int_group):
                    name = 'thread' + str(i)
                    locals()[name] = threading.Thread(target=RequestDynaDataFromInt, name='get_data',
                                                      args=[self.scats_id_list[i], self.int_str_input, i])
                    print('thread %s is running...' % i)
                    locals()[name].start()
                    threads.append(locals()[name])
                    # print(threading.current_thread().name)
                    # n += 1
                    # thread.join()
                for t in threads:
                    t.join()

    def loss_data_period_check(self, interface_name, stime, etime):

        """
        缺失数据时段检测
        :return:
        """
        pg = Postgres(SqlText.pg_inf_arith)
        result = pg.call_pg_data(sql_loss_data_period.format(interface_name, stime, etime), fram=True)
        if result is not None and not result.empty:
            result_loss = result[result['data_num'] == 0]
            result = result[result['data_num'] != 0]
            data_num = result['data_num'].values
            avg_num = np.mean(data_num)
            dev = np.std(data_num)
            print("%s今日各时段平均请求量:%s;标准差：%s" % (interface_name, avg_num, dev))
            if avg_num > 0:
                result_loss.append(result[result['data_num'] < avg_num - 2 * dev])
            else:
                result_loss.append(result)
            print(result_loss)
            loss_time = result_loss['record_time'].apply(lambda x: str(x)).values.tolist()
            return loss_time

    @timer
    def operate_request(self):
        """
        对一天的操作记录进行请求
        :return:
        """
        # loss_time = self.loss_data_period_check('人工操作记录接口')
        # if loss_time is not None:
        #     for time in loss_time:
        #         start_time = dt.datetime.strftime(dt.datetime.strptime(time, '%Y-%m-%d %H:%M:%S') -
        #                                                       dt.timedelta(minutes=15), '%Y-%m-%d %H:%M:%S')
        #         end_time = time
        #         operate_request(start_time, end_time)
        stime = (dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y-%m-%d') + ' 00:00:00'
        etime = (dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y-%m-%d') + ' 23:59:59'
        operate_request(stime, etime)


def main():
    I = InterfaceCheck()
    I.salk_list_request()
    I.operate_request()
