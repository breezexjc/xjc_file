from apscheduler.schedulers.background import BackgroundScheduler
from django_apscheduler.jobstores import DjangoJobStore, register_events
from ..python_project.ali_alarm.main import main
from ..python_project.ali_alarm.alarm_priority_algorithm2.SituationOperate import so_run
from ..python_project.scats_interface.runStrategicinfo_getdata import thread_creat
from ..python_project.scats_interface.scats_5min_volumns import RequestDynaDataFromInt
from ..python_project.scats_interface.Request_Data_From_Int import RequestDynaDataFromInt as get_operate
# from proj.python_project.scats_operate_parsing.seperate_operate_record import main as operate_resolve

import cx_Oracle
import pandas as pd
import numpy as np
# from proj.config.log_record import LogRecord
import logging
import datetime as dt
import queue
detail_queue = queue.Queue(maxsize=1000)
logger = logging.getLogger('schedulerTask')  # 获取settings.py配置文件中logger名称
from ..python_project.scats_operate_parsing import seperate_operate_record


# d = DjangoJobStore()
# d.remove_all_jobs()



def time_task(task):
    date = dt.datetime.now()
    print(task, "I'm a test job1236!", date)


class CONSTANT:
    IF_DATA_REPAIR = False
    S_REPAIR_DATE = '2018-10-14 20:30:00'
    E_REPAIR_DATE = '2018-10-15 15:00:00'
    request_interval = 300
    group_interval = 200
    TimeDelta = 360
    TimeDelay = 0
    OracleUser = 'enjoyor/admin@33.83.100.139/orcl'
    # OracleUser = 'SIG_OPT_ADMIN/admin@192.168.20.56/orcl'
    pg_inf = {'database': "superpower",'user': "postgres",'password': "postgres",
              'host': "172.20.251.98",'port': "5432"}


def CallOracle():
    rs1 = []
    match_records = []
    try:  # 数据库连接超时即退出程序
        db = cx_Oracle.connect(CONSTANT.OracleUser)
        cr = db.cursor()
    except cx_Oracle.DatabaseError:
        print('ERROR:数据库连接超时')
        # sys.exit(0)
    else:
        try:  # 表名错误或日期错误即退出
            sql1 = " select * from INTERSECT_INFORMATION order by SITEID "
            cr.execute(sql1)
            rs1 = cr.fetchall()
        except cx_Oracle.DatabaseError:
            print('ERROR:数据表名输入错误或不存在')
            # sys.exit(0)
        else:
            match_records = pd.DataFrame(rs1)
            # print(match_records)
            match_records.columns = ['SITEID', 'SITENAME', 'REGION']
        finally:
            cr.close()
            db.close()
    return match_records


def get_scats_int():
    def int_grouped(node_num, group):
        node_list = []
        for i in range(int(group)):
            try:
                select_node = node_num[i * CONSTANT.group_interval:(i + 1) * CONSTANT.group_interval]
            except Exception as e:
                select_node = node_num[i * CONSTANT.group_interval:]
                # select_node = node_num[i*100:]
                print(e)
            # print(select_node)
            node_list.append(select_node)
        # print(node_list)
        return node_list
    IntersectInfo = CallOracle()  # 从数据库读取路口列表
    currenttime = dt.datetime.now()
    if len(IntersectInfo) > 0:
        IntersectIDlist = IntersectInfo['SITEID']
        try:
            conn = cx_Oracle.connect(CONSTANT.OracleUser)  # 连接数据库
        except Exception as e:
            print('MainProcess:连接数据库失败', e)
        else:
            cr = conn.cursor()  # 建立游标
            try:
                cr.execute("SELECT * FROM INT_STR_INPUT order by SITEID")  # 从Oracle中读取数据
                IntStrInput = cr.fetchall()
            except Exception as e:
                print("oracle连接失败", e)
            else:
                conn.commit()
                # print(IntersectIDlist)
                # print(group)
                int_id = np.array(IntersectIDlist).tolist()
                int_num = len(int_id)
                print("请求总路口数：", int_num)
                group = round(int_num / CONSTANT.group_interval, 0) + 1
                int_grouped_data = int_grouped(int_id, group)
                return group, int_grouped_data,IntStrInput

            finally:
                cr.close()
                conn.close()
    else:
        print('获取节点列表失败')


def create_scheduler():
    # manage = SchedulerManage()
    scheduler = BackgroundScheduler(daemonic=True)
    scheduler.add_jobstore(DjangoJobStore(), "default")
    date = dt.datetime.now()
    # scheduler.add_job(main, "date", run_date=date, id='alarm_proj', args=[], replace_existing=True)
    # scheduler.add_job(seperate_operate_record.main, "date", run_date=date, id='operate_parsing', args=[], replace_existing=True)
    # scheduler.add_job(operate_resolve, "date", run_date=date, id='alarm_proj', args=[], replace_existing=True)
    # scheduler.add_job(seperate_operate_record.main, "interval", minutes=1, id='operate_proj', args=[])
    # scheduler.add_job(time_task, "interval", seconds=5, id='mytask2', args=['mytask2',], replace_existing=True)
    # scheduler.add_job(so_run, "interval", minutes=1, id='operate_match', args=[], replace_existing=True)
    try:
        group, int_list, scats_input = get_scats_int()
    except Exception as e:
        logger.error(e)
        print(e)
    else:
        logger.info("get scats basic inf successfully!")
        scheduler.add_job(thread_creat, "interval", minutes=5, id='scats_salklist', args=[group, int_list, scats_input,detail_queue],
                          replace_existing=True)
        scheduler.add_job(RequestDynaDataFromInt, "interval", minutes=5, id='scats_volumns', args=[int_list,detail_queue],
                          replace_existing=True)
        scheduler.add_job(get_operate, "interval", minutes=3, id='scats_operate', args=[detail_queue],
                          replace_existing=True)
    scheduler.start()
    logger.info('start scheduler task')
    print("=======================定时任务启动==========================")
    print(scheduler.get_jobs())
    print(scheduler.state)
    logger.info('start task register,check on admin platform!')
    register_events(scheduler)
    # scheduler.add_executor()
    # return manage

# message_queue = queue.Queue(10)
manage = create_scheduler()

def getScheduler(request):
    json_demo = {'appcode': False, 'result': []}
    if request.GET:
        json_demo2 = {'appcode': True, 'result': []}

        if 'TaskId' in request.GET:
            task_id = request.GET['TaskId']
            task_state = SchedulerManage.jobstate.get(task_id)
            json_demo2['result'].append(task_state)
        else:
            json_demo2['appcode'] = False
            # json_result = json.dumps(json_demo2, ensure_ascii=False)
        response = JsonResponse(json_demo2, safe=False, json_dumps_params={'ensure_ascii': False})
        return response
    else:
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response


