from apscheduler.schedulers.background import BackgroundScheduler
from django_apscheduler.jobstores import DjangoJobStore, register_events, register_job
import datetime as dt
from ..python_project.ali_alarm.main import main
from ..python_project.ali_alarm.alarm_priority_algorithm2.SituationOperate import so_run
from ..python_project.scats_interface.runStrategicinfo_getdata import thread_creat
from ..python_project.scats_interface.scats_5min_volumns import RequestDynaDataFromInt
from ..python_project.scats_interface.Request_Data_From_Int import RequestDynaDataFromInt as get_operate

import cx_Oracle
import pandas as pd
import numpy as np
# from proj.config.log_record import LogRecord
import logging
import queue
import datetime as dt

logger = logging.getLogger('sourceDns.webdns.views')  # 获取settings.py配置文件中logger名称
# from ..python_project.scats_operate_parsing import seperate_operate_record


# d = DjangoJobStore()
# d.remove_all_jobs()


class SchedulerManage():
    jobstate = {}
    def __init__(self,):
        self.jobstate = {}
        pass

    def message_accept(self, message_queue):
        while True:
            if message_queue.qsize() > 0:
                message = message_queue.get()
                job_name = message.get('name')
                job_state = message.get('state')
                localtime = dt.datetime.now()
                self.jobstate[job_name] = {'time': localtime, 'state': job_state}


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
    # OracleUser = 'enjoyor/admin@33.83.100.139/orcl'
    OracleUser = 'SIG_OPT_ADMIN/admin@192.168.20.56/orcl'

    pg_inf = {'database': "superpower",'user': "postgres",'password': "postgres",
              'host': "172.20.251.98",'port': "5432"}
    # 数据库插入语句
    # 4.1
    sql_IntInfo = "insert into GET_INT_INFORMATION(Detector,PhaseNo,Region,SINo,SiteID,SiteName,SubSystemID)  " \
                  "values (:1,:2,:3,:4,:5,:6,:7)"
    sqld_IntInfo = "delete * from GET_INT_INFORMATION"
    # 4.2
    sql_FlowInfo = "insert into GET_FLOW_INFORMATION(DetectorNo,Flow,FlowTime,SiteID,TimeDistance)  " \
                   "values (:1,:2,:3,:4,:5)"
    # 4.3
    sql_RunStrInfo = "insert into RUN_STR_INFORMATION1(RecvDate, RecvTime, A, ActiveLinkPlan, ActiveSystemPlan, " \
                     "ActualCycleTime, B, C, D, E, F, G, Id, IsSALK, NominalCycleTime, Region, RequiredCycleTime, " \
                     "Saturation, SiteID_T, StrategicCycleTime, SubSystemID) " \
                     "values (:0,:1,:3,:4,:5,:2,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20)"
    sql_RunStrInfoSalkList = "insert into RUN_STR_INFORMATION_SALKLIST1(RecvDate, RecvTime, SITEID_T, ADS, DS1, DS2, " \
                             "DS3, DS4, ID, ISSALK, PHASEBITMASK, PHASETIME, SALKNO, SITEID, VK1, VK2, VK3, VK4, " \
                             "VO1, VO2, VO3, VO4, SMID) values (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14," \
                             ":15,:16,:17,:18,:19,:20,:21,:22)"
    # 4.4
    sql_IntSplit = "insert into GET_INT_GSIGN(A,B,C,D,E,F,G,KeyPhase,LightgroupCount,PhaseCount,PhaseSort,PhaseTime," \
                   "PlanNo,SiteID)  values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)"
    sqld_IntSplit = "delete * from GET_INT_GSIGN"
    # 4.5
    sql_IntCycle = "insert into GET_INT_CYCLE (HCL,LCL,REGION,SCL1,SCL2,SZ1,SZ2,SUBSYSTEMID,XCL)" \
                   "values(:1,:2,:3,:4,:5,:6,:7,:8,:9)"
    sqld_IntCycle = "delete * from GET_INT_CYCLE"
    # 4.6
    sql_StrInput = "insert into INT_STR_INPUT(Detector,DiretionName,Lane1,Lane2,Lane3,Lane4,LaneNumber,PhaseNo," \
                   "SINo,SiteID, TrafficFlowDir)  values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11)"
    sqld_StrInput = "delete * from INT_STR_INPUT"
    # 4.7
    sql_ManoperationRecord = "insert into GET_MANOPERATION_RECORD(Oper, OperCode, OperTime, OperType, Region, " \
                             "SiteID, Userid) values (:1,:2,:3,:4,:5,:6,:7)"
    # 4.8
    sql_RealTimePhase = "insert into GET_REALTIME_PHASE(CurrentPhase, CurrentPhaseInterval, Cyclelength, " \
                        "ElapsedPhaseTime, NextPhase, NominalCycleLength, OffsetPlanLocked, OffsetPlanNumbers, " \
                        "RemainingPhaseTime, RequiredCycle, SiteId, SplitPlanLocked, SplitPlanNumbers, " \
                        "SubsystemNumber) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)"
    sqld_RealTimePhase = "delete * from GET_REALTIME_PHASE"
    # 解析
    sql_AnalyzedRunInfo = "insert into HZ_SCATS_OUTPUT(FSTR_INTERSECTID, FINT_SA, FINT_DETECTORID, " \
                          "FSTR_CYCLE_STARTTIME, FSTR_PHASE_STARTTIME, FSTR_PHASE, FINT_PHASE_LENGTH, " \
                          "FINT_CYCLE_LENGTH, FINT_DS, FINT_ACTUALVOLUME, FSTR_DATE,FSTR_WEEKDAY, FSTR_CONFIGVERSION) " \
                          "values (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)"
    sql_AnalyzedRunInfoSalkList = "insert into XIEJC.SKITLIST_TEXT(A, ActiveLinkPlan, ActiveSystemPlan, " \
                                  "ActualCycleTime, B, C, D, E, F, G, Id, IsSALK, NominalCycleTime, RecvTime, " \
                                  "Region, RequiredCycleTime, Saturation, SiteID_T, StrategicCycleTime, SubSystemID)" \
                                  " values (:1,:2,:3,:0,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19) "

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


def create_scheduler(message_queue):
    manage = SchedulerManage()
    scheduler = BackgroundScheduler(daemonic=True)
    scheduler.add_jobstore(DjangoJobStore(), "default")
    date = dt.datetime.now()
    scheduler.add_job(main, "date", run_date=date, id='alarm_proj', args=[message_queue], replace_existing=True)
    # scheduler.add_job(manage.message_accept, "date", run_date=date, id='alarm_proj', args=[message_queue], replace_existing=True)
    # scheduler.add_job(seperate_operate_record.main, "interval", minutes=1, id='operate_proj', args=[])
    # scheduler.add_job(time_task, "interval", seconds=5, id='mytask2', args=['mytask2',], replace_existing=True)
    scheduler.add_job(so_run, "interval", minutes=1, id='operate_match', args=[message_queue], replace_existing=True)

    try:
        group, int_list, scats_input = get_scats_int()
    except Exception as e:
        logger.error(e)
        print(e)
    else:
        logger.info("scats基础信息获取成功")
        scheduler.add_job(thread_creat, "interval", minutes=5, id='scats_salklist', args=[group, int_list, scats_input],
                          replace_existing=True)
        scheduler.add_job(RequestDynaDataFromInt, "interval", minutes=5, id='scats_volumns', args=[int_list],
                          replace_existing=True)
        scheduler.add_job(get_operate, "interval", minutes=3, id='scats_operate', args=[],
                          replace_existing=True)

    scheduler.start()
    logger.error('定时任务开始')
    print("=======================定时任务启动==========================")
    print(scheduler.get_jobs())

message_queue = queue.Queue(10)
create_scheduler(message_queue)
