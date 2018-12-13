from proj.config.database import Oracle, Postgres
from proj.python_project.scats_interface.pram_conf import *
import datetime as dt
import cx_Oracle
import pandas as pd
from proj.tools.func_timer import timer

# def time_count(func):
#     def warpper(*args, **kwargs):
#         stime = dt.datetime.now()
#         res = func(*args, **kwargs)
#         etime = dt.datetime.now()
#         print("[DeBug] {0} CostTime: {1} ".format(func.__name__, (etime - stime).seconds))
#         return res
#
#     return warpper


class InterfaceStatus():
    OracleUser = 'enjoyor/admin@192.168.20.56/orcl'
    pg_inf = {'database': "arithmetic", 'user': "django", 'password': "postgres",
              'host': "192.168.20.46", 'port': "5432"}

    def __init__(self):
        self.status = None
        self.pg = Postgres(InterfaceStatus.pg_inf)
        self.ora = Oracle(InterfaceStatus.OracleUser)
        self.conn, self.cr = self.ora.db_conn()

    @timer
    def salklist_status(self, date, stime, etime):

        try:
            self.cr.execute(sql_get_salklist_status, a=date, b=stime, f=etime)
        except cx_Oracle.InterfaceError:
            self.conn, self.cr = self.ora.db_conn()
            self.cr.execute(sql_get_salklist_status, a=date, b=stime, f=etime)

        result = self.cr.fetchall()
        # print(result)
        self.conn.commit()
        # self.ora.db_close()
        data_num = result[0][0]
        return data_num

    @timer
    def operate_status(self, stime, etime):
        stime = dt.datetime.strptime(stime, '%Y-%m-%d %H:%M:%S')
        etime = dt.datetime.strptime(etime, '%Y-%m-%d %H:%M:%S')

        try:
            self.cr.execute(sql_get_opetate_status, a=stime, b=etime)
        except cx_Oracle.InterfaceError:
            self.conn, self.cr = self.ora.db_conn()
            self.cr.execute(sql_get_opetate_status, a=stime, b=etime)
        result = self.cr.fetchall()
        # print(result)
        self.conn.commit()
        data_num = result[0][0]
        return data_num

    @timer
    def parsing_failed_check(self, date):
        # stime = dt.datetime.strptime(stime, '%Y-%m-%d %H:%M:%S')
        # etime = dt.datetime.strptime(etime, '%Y-%m-%d %H:%M:%S')
        try:
            self.cr.execute(sql_failed_detector, a=date, b=parse_failed_judge)

        except cx_Oracle.InterfaceError:
            self.conn, self.cr = self.ora.db_conn()
            self.cr.execute(sql_failed_detector, a=date, b=parse_failed_judge)

        result = self.cr.fetchall()
        if IF_TEST:
            result = list(result)
            for i in range(len(result)):
                result[i] = list(result[i])
                result[i][3] = str(dt.datetime.now().date())

        # print(result)
        self.conn.commit()
        return result

    def salk_send(self, delta=15):
        current_time = dt.datetime.now()
        start_time = current_time - dt.timedelta(minutes=delta)
        current_date = str(current_time.date())
        if IF_TEST:
            current_date = TEST_DATE
            test_time = TEST_DATE + ' ' + str(current_time.strftime('%H:%M:%S'))
            start_time = dt.datetime.strptime(test_time, "%Y-%m-%d %H:%M:%S") - dt.timedelta(minutes=delta)
        # print(current_date, start_time.strftime("%H:%M:%S"), current_time.strftime("%H:%M:%S"))
        salk_num = self.salklist_status(current_date, start_time.strftime("%H:%M:%S"),
                                        current_time.strftime("%H:%M:%S"))
        message_salk = [['战略运行记录接口', current_time, salk_num]]
        self.pg.send_pg_data(sql=sql_send_message, data=message_salk)
        return

    def operate_send(self, delta=15):
        current_time = dt.datetime.now()
        start_time = current_time - dt.timedelta(minutes=delta)
        if IF_TEST:
            test_time = TEST_DATE + ' ' + str(current_time.strftime('%H:%M:%S'))
            test_time = dt.datetime.strptime(test_time, "%Y-%m-%d %H:%M:%S")
            test_start_time = current_time - dt.timedelta(minutes=delta)
            operate_num = self.operate_status(test_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                                              test_time.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            operate_num = self.operate_status(start_time.strftime("%Y-%m-%d %H:%M:%S"),
                                              current_time.strftime("%Y-%m-%d %H:%M:%S"))
        message_operate = [['人工操作记录接口', current_time, operate_num]]
        self.pg.send_pg_data(sql=sql_send_message, data=message_operate)
        return

    def parse_failed_detector_send(self):
        current_time = dt.datetime.now()
        # current_date = current_time.date()
        current_date = str(current_time.date())
        if IF_TEST:
            current_date = '2018-11-09'
        failed_detector = self.parsing_failed_check(current_date)
        self.pg.send_pg_data(sql=sql_send_parse_failed_detector, data=failed_detector)
        return


def main(delta):
    I = InterfaceStatus()
    I.salk_send(delta)
    I.operate_send(delta)
    I.parse_failed_detector_send()
    I.ora.db_close()


if __name__ == "__main__":
    main(15)
    # current_time = dt.datetime.now()
    # I = InterfaceStatus()
    # salk_num = I.salklist_status('2018-11-05', '09:00:00', '10:00:00')
    # operate_num = I.operate_status('2018-11-05 09:00:00', '2018-11-05 10:00:00')
    # message_salk = [['战略运行记录接口', current_time, salk_num, None]]
    # I.pg.send_pg_data(sql=sql_send_message, data=message_salk)
    # message_operate = [['人工操作记录接口', current_time, operate_num, None]]
    # I.pg.send_pg_data(sql=sql_send_message, data=message_operate)
    # failed_detector = I.parsing_failed_check('2018-11-05')
    # I.pg.send_pg_data(sql=sql_send_parse_failed_detector, data=failed_detector)
    # df_failed_detector = pd.DataFrame(failed_detector, columns=['scats_id', 'sa_no', 'detector'])
    # print("salk_num:", salk_num)
    # print("operate_num:", operate_num)
    # print("parsing_failed_check:", len(failed_detector))
    # # json = df_failed_detector.to_json(orient='records')
    # dict = df_failed_detector.to_dict(orient='records')
    # print(dict)
