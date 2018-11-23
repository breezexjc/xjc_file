from proj.python_project.ali_alarm.alarm_priority_algorithm3.alarm_data_regular_filter import new_kde_cal

import datetime as dt
from proj.config.database import Postgres
import logging
from proj.python_project.ali_alarm.alarm_priority_algorithm3.control_pram import *
logger = logging.getLogger('schedulerTask')  # 获取settings.py配置文件中logger名称


class SqlContent:
    sql_get_operate_dispose = """
 SELECT
	distinct fat.inter_id,son.date_day+son.date_time,fat.reason_type,fat.alarm_id,son.disp_type,son.inter_name
FROM
	disposal_classify fat
RIGHT JOIN (
	SELECT
		inter_id,
		MAX (time_point) AS time_point 
	FROM
		disposal_classify
	where  inter_id in ({0}) and time_point > '{1}' and reason_type !=10
	GROUP BY
		inter_id
) grl
on
	fat.inter_id = grl.inter_id
AND fat.time_point = grl.time_point
LEFT JOIN disposal_data son on fat.alarm_id = son.alarm_id

"""
    sql_get_int_type = """
SELECT
	gaode_id,
	int_alarm_type
FROM
	alarm_int_alarm_type
WHERE
	gaode_id IN ({0})"""


class OperateAutoDis():
    # pg_inf = {'database': "research", 'user': "django", 'password': "postgres",
    #           'host': "192.168.20.45", 'port': "5432"}
    # pg_inf = {'database': "signal_specialist", 'user': "django", 'password': "postgres",
    #           'host': "192.168.20.46", 'port': "5432"}
    intid_list = ['14KC7097AL0', '14KOB0981K0']

    def __init__(self):
        self.pg = Postgres()
        self.int_auto = {}

    def get_int_type(self, intid_list=None):
        if IF_TEST:
            intid_list = OperateAutoDis.intid_list
        str_intid_list = ['\'' + int + '\'' for int in intid_list]
        pram = ','.join(str_intid_list)
        # print(pram)
        result = self.pg.call_pg_data(SqlContent.sql_get_int_type.format(pram))
        print('get_int_type', result)
        return result

    def get_alarm_type(self, intid_list=None):
        if IF_TEST:
            intid_list = OperateAutoDis.intid_list

        result = new_kde_cal()
        print(result)
        return result

    def get_alarm_operate_type(self, intid_list=None):
        if IF_TEST:
            intid_list = OperateAutoDis.intid_list
        str_intid_list = ['\'' + int + '\'' for int in intid_list]
        pram = ','.join(str_intid_list)
        current_date = dt.datetime.now().date()
        stime = str(current_date) + ' 00:00:00'
        print(pram)
        result = self.pg.call_pg_data(SqlContent.sql_get_operate_dispose.format(pram, stime), fram=True)
        print('operate', result)
        return result

    def alarm_auto_set(self, alarm_dispose_data):
        auto = None
        # TD = 30
        # TA = 16
        # TW = 16
        # TRD = 10
        for i in alarm_dispose_data:
            (int_id, time_point, auto_dis, alarm_id, dis_type, int_name) = i
            current_time = dt.datetime.now()
            time_delta = (current_time - dt.datetime.strptime(str(time_point), '%Y-%m-%d %H:%M:%S')).seconds
            current_time = dt.datetime.now()
            # 关注
            if dis_type == 1 and time_delta < 60 * TA:
                end_time = dt.datetime.strptime(str(time_point), '%Y-%m-%d %H:%M:%S') + dt.timedelta(
                    seconds=60 * TA)
                self.int_auto[int_id] = {'lastDis': '1', 'auto': 1, 'endTime': end_time}
                auto = True
            # 调控,调控完以后，30分钟路口不自动处置
            elif dis_type == 2 and time_delta < 60 * TD:
                end_time = dt.datetime.strptime(str(time_point), '%Y-%m-%d %H:%M:%S') + dt.timedelta(
                    seconds=60 * TD)
                self.int_auto[int_id] = {'lastDis': '2', 'auto': 0, 'endTime': end_time}
                auto = False
            # 误报
            elif dis_type == 3 and time_delta < 60 * TW:
                end_time = dt.datetime.strptime(str(time_point), '%Y-%m-%d %H:%M:%S') + dt.timedelta(
                    seconds=60 * TW)
                self.int_auto[int_id] = {'lastDis': '3', 'auto': 1, 'endTime': end_time}
                auto = True
            else:
                # 无处置记录
                # end_time = current_time + dt.timedelta(
                #     seconds=60 * TIME_DELAY_N)
                # self.int_auto[int_id] = {'lastDis': '5', 'auto': 0, 'endTime': end_time}
                auto = False
        return auto

    def alarm_auto_judge(self, alarm_int_list):
        """
        :param alarm_int_list: 报警路口列表
        :return: 报警是否自动处置结果
        lastDis：上一次路口处置状态【1：关注；2：调控；3：误报；4：快处；5：推送人工】
        """
        reponse_all = []
        # alarm_type 报警常发偶发判断结果
        try:
            self.frame_int_state = self.get_alarm_operate_type(alarm_int_list)
            int_type = self.get_int_type(alarm_int_list)
            alarm_type = self.get_alarm_type()
        except Exception as e:
            print("获取路口报警类型失败 或 计算报警强度失败")
            print(e)
            int_type = None
        else:
            int_type = dict(int_type)
            if alarm_type:
                fre_alarm_list = [i[0] for i in alarm_type if i[4] == '0' or i[4] == '0.0']
            else:
                fre_alarm_list = []
            for int in alarm_int_list:
                int_alarm_type = int_type.get(int)
                # 报警较少路口
                if int_alarm_type == '0':
                    reponse_all = self.continue_alarm_judge(int, reponse_all)
                # 报警较多路口
                elif int_alarm_type == '1':
                    # 常发报警
                    if int in fre_alarm_list:
                        reponse_all = self.man_disposal_judge(int, reponse_all)
                    # 偶发报警
                    else:
                        reponse_all = self.continue_alarm_judge(int, reponse_all)

        return reponse_all

    def auto_disposal_delta(self, int, reponse_all):

        int_key = self.int_auto.keys()
        int_check_state = []
        current_time = dt.datetime.now()
        if int not in int_key:
            new_end_time = current_time + dt.timedelta(seconds=TRD * 60)
            self.int_auto[int] = {'lastDis': '4', 'auto': 0, 'endTime': new_end_time}
            reponse = {'alarmInt': int, 'autoDis': True}
            reponse_all.append(reponse)
        else:
            int_auto_data = self.int_auto.get(int)
            last_dis = int_auto_data.get('lastDis')
            auto = int_auto_data.get('auto')
            end_time = int_auto_data.get('endTime')
            if current_time > end_time:
                # 路口超时后，自动处置第一次报警！同时将路口状态设置为自动处置
                new_end_time = current_time + dt.timedelta(seconds=TRD * 60)
                self.int_auto[int] = {'lastDis': '4', 'auto': 0, 'endTime': new_end_time}
                reponse = {'alarmInt': int, 'autoDis': True}
                reponse_all.append(reponse)

            elif (last_dis == '4' or last_dis == '5') and auto == 0 and current_time <= end_time:
                # 规定时间间隔内，上一次处置为自动处置
                # 1、判断路口类型；2、判断路口报警类型
                int_check_state.append(int)
                pass

            elif auto == 1 and current_time <= end_time:
                # 路口被设置为自动处置，且未超时
                reponse = {'alarmInt': int, 'autoDis': True}
                reponse_all.append(reponse)

    def man_disposal_judge(self, int, reponse_all):
        """
        :param int: 需要查询的路口
        :param reponse_all: 判断结果列表
        :return: 判断结果列表
        根据人工处置记录判断是否快速处置
        """
        frame_int_state = self.frame_int_state
        try:
            match_dis_type = frame_int_state[frame_int_state['inter_id'] == int].values
        except Exception as e:
            print(e)
        else:
            # 若路口匹配到了调控记录
            auto = self.alarm_auto_set(match_dis_type)
            if auto is True:
                reponse = {'alarmInt': int, 'autoDis': True}
                reponse_all.append(reponse)

            elif auto is False:
                reponse = {'alarmInt': int, 'autoDis': False}
                reponse_all.append(reponse)
            else:
                # 匹配不到操作记录，则推送人工
                reponse = {'alarmInt': int, 'autoDis': False}
                reponse_all.append(reponse)

        return reponse_all

    def continue_alarm_judge(self, int, reponse_all):
        int_key = self.int_auto.keys()
        current_time = dt.datetime.now()
        if int not in int_key:
            new_end_time = current_time + dt.timedelta(seconds=TRD * 60)
            self.int_auto[int] = {'lastDis': '4', 'auto': 0, 'endTime': new_end_time}
            reponse = {'alarmInt': int, 'autoDis': True}
            reponse_all.append(reponse)
        else:
            int_auto_data = self.int_auto.get(int)
            last_dis = int_auto_data.get('lastDis')
            auto_signal = int_auto_data.get('auto')
            end_time = int_auto_data.get('endTime')
            if current_time > end_time:
                # 路口超时后，自动处置第一次报警！同时将路口状态设置为自动处置
                new_end_time = current_time + dt.timedelta(seconds=TRD * 60)
                self.int_auto[int] = {'lastDis': '4', 'auto': 0, 'endTime': new_end_time}
                reponse = {'alarmInt': int, 'autoDis': True}
                reponse_all.append(reponse)

            elif (last_dis == '4' or last_dis == '5') and auto_signal == 0 and current_time <= end_time:
                # 规定时间间隔内，上一次处置为自动处置
                # 1、判断路口类型；2、判断路口报警类型
                reponse_all = self.man_disposal_judge(int, reponse_all)
                pass

            elif auto_signal == 1 and current_time <= end_time:
                # 路口被设置为自动处置，且未超时
                reponse = {'alarmInt': int, 'autoDis': True}
                reponse_all.append(reponse)
        return reponse_all


if __name__ == "__main__":
    O1 = OperateAutoDis()
    # O1.get_alarm_operate_type()
    result = O1.alarm_auto_judge(['14KC7097AL0', '14LMM097HE0'])
    result2 = O1.alarm_auto_judge(['14KC7097AL0', '14LMM097HE0'])
    print(result2)
