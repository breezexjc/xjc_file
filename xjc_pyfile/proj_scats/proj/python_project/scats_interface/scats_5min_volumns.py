# -*- coding: utf-8 -*-
#############################
# Created on Wed Nov 1 2017
# Description: 共八个接口，历史数据（1，4，5，6，8）在开始请求一次，实时数据（2，3，7）每2分钟请求一次，原始数据和解析数据分别存入数据库
# @author: A
#############################


import sys
import requests
import datetime as dt
import cx_Oracle
import pandas as pd
import threading
import json
from datetime import datetime
import logging
Log = logging.getLogger('scats')  # 获取settings.py配置文件中logger名称

class CONSTANT:
    TimeDelta = 600
    TimeDelay = 180

    OracleUser = 'enjoyor/admin@33.83.100.139/orcl'
    # OracleUser = 'SIG_OPT_ADMIN/admin@192.168.20.56/orcl'
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
                          "FINT_CYCLE_LENGTH, FINT_DS, FINT_ACTUALVOLUME, FSTR_DATE,FSTR_WEEKDAY, FSTR_CONVIGVERSION) " \
                          "values (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)"
    sql_AnalyzedRunInfoSalkList = "insert into XIEJC.SKITLIST_TEXT(A, ActiveLinkPlan, ActiveSystemPlan, " \
                                  "ActualCycleTime, B, C, D, E, F, G, Id, IsSALK, NominalCycleTime, RecvTime, " \
                                  "Region, RequiredCycleTime, Saturation, SiteID_T, StrategicCycleTime, SubSystemID)" \
                                  " values (:1,:2,:3,:0,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19) "


# 连接数据库读取路口list
def CallOracle():
    try:  # 数据库连接超时即退出程序
        db = cx_Oracle.connect(CONSTANT.OracleUser)
        cr = db.cursor()
        try:  # 表名错误或日期错误即退出
            sql1 = " select * from INTERSECT_INFORMATION order by SITEID "
            cr.execute(sql1)
            rs1 = cr.fetchall()
            match_records = pd.DataFrame(rs1)
            match_records.columns = ['SITEID', 'SITENAME', 'REGION']
            # db.close()
        except cx_Oracle.DatabaseError:
            print('ERROR:数据表名输入错误或不存在')
            sys.exit(0)
    except cx_Oracle.DatabaseError:
        print('ERROR:数据库连接超时')
        sys.exit(0)
    return match_records


# 原始数据写入数据库
def WriteOriginalData(conn, itemlist, sql, sql_d):  # 原始数据写入数据库
    cursor = conn.cursor()
    try:
        for i in itemlist:
            if len(i):
                cursor.execute(sql, i)
    except cx_Oracle.IntegrityError:
        cursor1 = conn.cursor()
        cursor1.execute(sql_d)
        print('已删原历史数据')
        for i in itemlist:
            if len(i):
                cursor.execute(sql, i)
    return


# 动态数据写入数据库
def WriteDynaData(conn,cursor, itemlist, sql):  # 原始数据写入数据库
    for i in itemlist:
        if len(i):
            try:
                cursor.execute(sql, i)
            except Exception as e:
                conn.commit()
            else:
                pass
    return


# 向接口请求动态数据
def RequestDynaDataFromInt(siteIDlist):
    list_FlowInfo = []  # 4.2
    list_RunStrInfo = []  # 4.3
    list_RunStrInfoSalkList = []
    list_AnalyzedRunInfo = []  # 解析结果
    list_AnalyzedRunInfoSalkList = []
    list_ManoperationRecord = []  # 4.7

    Now = dt.datetime.now()  # 获取当前时间
    TimeDelta = dt.timedelta(seconds=CONSTANT.TimeDelta)  # 读接口的时间差
    # TempStartTime = Now - TimeDelta
    # TempEndTime = Now
    TimeDelay = dt.timedelta(seconds=CONSTANT.TimeDelay)
    StartTime = (Now - TimeDelta - TimeDelay).strftime('%Y-%m-%d %H:%M:%S')
    EndTime = (Now - TimeDelay).strftime('%Y-%m-%d %H:%M:%S')
    print('*开始获取5分钟流量')
    success_request = 0
    for m in range(len(siteIDlist)):
        SiteID = siteIDlist[m]
        payload1 = {'SiteID': SiteID, 'STime': StartTime, 'ETime': EndTime}
        try:
            # 4.2获取流量信息
            GetFlowInformation = requests.get('http://33.83.100.138:8080/getDetectorCounts.html',
                                          params=payload1).text  # 4.2
        except Exception as e:
            pass
            # Log.warning(e)
        else:
            success_request += 1
            if GetFlowInformation[-3] != '[':
                list_FlowInfo = runFlowInfo(GetFlowInformation, list_FlowInfo)
            else:
                pass
    Log.info('scats 5-minutes\'s volumn interface request success num=%s' % success_request)
    # 原始实时数据和解析数据分别写入数据库
    try:
        conn = cx_Oracle.connect(CONSTANT.OracleUser)
        cursor = conn.cursor()
    except Exception as e:
        Log.error(e)
    else:
        WriteDynaData(conn,cursor, list_FlowInfo, CONSTANT.sql_FlowInfo)  # 4.2
        Log.info('scats 5-minutes\'s volumn interface insert successfully!insert_num=%s' % len(list_FlowInfo))
        conn.commit()
        conn.close()

    # return GetIntInformation, GetFlowInformation, RunStrInformation, GetIntGSIN, GetIntCycle, IntStrInput\
    #     , GetManoperationRecord, GetRealTimePhase
    return list_RunStrInfo, list_RunStrInfoSalkList


# 4.2获取流量信息
def runFlowInfo(getflowinformation, list_FlowInfo):
    FlowInformation = json.loads(getflowinformation)
    FlowInfo = FlowInformation["resultList"]
    # list_FlowInfo = []
    if any(FlowInfo):
        for i in FlowInfo:
            DetectorNo = i['DetectorNo']
            Flow = i['Flow']
            FlowTime = i['FlowTime']
            SiteID = i['SiteID']
            TimeDistance = i['TimeDistance']
            itemlist = [DetectorNo, Flow, FlowTime, SiteID, TimeDistance]
            list_FlowInfo.append(itemlist)
    else:
        pass
    return list_FlowInfo


def SecondToTime(s):
    if s < 60:
        second = s
        minutes = 0
    else:
        second = s % 60
        minutes = s/60
    return '0:%d:%d' % (minutes, second)


def MainLoop():
    global timer
    timer = threading.Timer(CONSTANT.TimeDelta, MainLoop)
    timer.start()

    # GetIntInformation, GetFlowInformation, RunStrInformation, GetIntGSIN, GetIntCycle, IntStrInput, \
    # GetManoperationRecord, GetRealTimePhase = RequestDynaDataFromInt(IntersectSiteIDlist)  # 向接口请求动态数据
    RequestDynaDataFromInt(IntersectIDlist, IntStrInput)  # 向接口请求动态数据并解析

    return


def MainProcess():
    global IntersectIDlist  # 路口列表
    global IntStrInput  # 配置表

    IntersectInfo = CallOracle()  # 从数据库读取路口列表
    IntersectIDlist = IntersectInfo['SITEID']
    print(IntersectIDlist)
    # print(len(IntersectSiteIDlist))
    # IntStrInput = RequestHistoryDataFromInt(IntersectIDlist)  # 请求历史数据并写入数据库，返回解析所需配置表
    conn = cx_Oracle.connect(CONSTANT.OracleUser)  # 连接数据库('账号/密码 @192.168.20.62/orcl')
    cr = conn.cursor()  # 建立游标
    cr.execute("SELECT * FROM INT_STR_INPUT order by SITEID")  # 从Oracle中读取数据
    IntStrInput = cr.fetchall()
    # conn.close()

    # timer = threading.Timer(1, MainLoop(IntersectIDlist, IntStrInput))
    timer = threading.Timer(1, MainLoop)
    timer.start()
    return


