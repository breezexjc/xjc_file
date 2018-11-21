
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'  # 或者os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'
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
Log = logging.getLogger('scats')

class CONSTANT:
    ONEGROUP = 50
    interval = 180
    TimeDelta = 10*60
    TimeDelay = 0
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
                          "FINT_CYCLE_LENGTH, FINT_DS, FINT_ACTUALVOLUME, FSTR_DATE,FSTR_WEEKDAY, FSTR_CONFIGVERSION) " \
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
            # print(match_records)
            match_records.columns = ['SITEID', 'SITENAME', 'REGION']
            cr.close()
        except cx_Oracle.DatabaseError:
            print('ERROR:数据表名输入错误或不存在')
            sys.exit(0)
    except cx_Oracle.DatabaseError:
        print('ERROR:数据库连接超时')
        sys.exit(0)
    return match_records, db



# 原始数据写入数据库
def WriteOriginalData(conn, itemlist, sql, sql_d):  # 原始数据写入数据库
    cursor = conn.cursor()
    try:
        for i in itemlist:
            if len(i):
                try:
                    cursor.execute(sql, i)
                    conn.commit()
                except cx_Oracle.IntegrityError as e:
                    conn.commit()
                    # print(e)
                    pass

    except cx_Oracle.IntegrityError as e:
        pass
        # cursor1 = conn.cursor()
        # cursor1.execute(sql_d)
        # print('已删原历史数据')
        # for i in itemlist:
        #     if len(i):
        #         cursor.execute(sql, i)
    cursor.close()
    return


# 动态数据写入数据库
def WriteDynaData(conn, itemlist, sql):  # 原始数据写入数据库
    cursor = conn.cursor()
    wrong = 0
    for i in itemlist:
        if len(i):
            conn.commit()
            try:
                cursor.execute(sql, i)
            except cx_Oracle.IntegrityError as e:
                wrong += 1
                conn.commit()
            except Exception as e:
                print(i, '\n', e)
                wrong += 1
    Log.info("成功插入：%s ,插入失败：%s" % (len(itemlist)-wrong, wrong))
    # print("成功插入：%s ,插入失败：%s" % (len(itemlist)-wrong, wrong))
    cursor.close()
    return
#
#
# # 向接口请求历史数据
# def RequestHistoryDataFromInt(IntersectIDlist,conn):
#     list_IntInfo = []  # 4.1
#     list_IntSplit = []  # 4.4
#     list_IntCycle = []  # 4.5
#     list_StrInput = []  # 4.6
#     list_RealTimePhase = []  # 4.8
#     GetIntInformation = []
#     GetIntGSIN = []
#     GetIntCycle = []
#
#     for m in range(len(IntersectIDlist)):
#         SiteID = IntersectIDlist[m]
#         payload2 = {'SiteID': SiteID}
#         # try:
#         # 4.1获取路口基础信息
#         try:
#             GetIntInformation = requests.get('http://33.83.100.138:8080/getSiteInfo.html', params=payload2).text  # 4.1
#         except Exception as e:
#             print(e)
#         if len(GetIntInformation) > 0:
#             list_IntInfo = runBasicInfo(GetIntInformation, list_IntInfo)
#         else:
#             pass
#         # print('4.1')
#
#         # 4.4获取路口绿信比
#         try:
#             GetIntGSIN = requests.get('http://33.83.100.138:8080/getSplitsPlan.html', params=payload2).text  # 4.4
#         except Exception as e:
#             print(e)
#         if len(GetIntGSIN) > 0:
#             list_IntSplit = runGsign(GetIntGSIN, list_IntSplit)
#         else:
#             pass
#
#         # print('4.4')
#
#         # 4.5获取路口周期
#         try:
#             GetIntCycle = requests.get('http://33.83.100.138:8080/getCyclePlan.html', params=payload2).text  # 4.5
#         except Exception as e:
#             print(e)
#         if len(GetIntCycle) > 0:
#             list_IntCycle = runCycle(GetIntCycle, list_IntCycle)
#         else:
#             pass
#
#         # print('4.5')
#
#
#     if len(list_IntCycle) == 0:
#         print("list_IntCycle接口获取数据失败")
#     if len(list_IntSplit) == 0:
#         print("list_IntSplit接口获取数据失败")
#     if len(list_IntInfo) == 0:
#         print("list_IntInfo接口获取数据失败")
#     # 历史数据写入数据库
#
#
#         # print(list_IntInfo)
#     if len(GetIntInformation) > 0:
#         WriteOriginalData(conn, list_IntInfo, CONSTANT.sql_IntInfo, CONSTANT.sqld_IntInfo)  # 4.1
#     if len(GetIntGSIN) > 0:
#         WriteOriginalData(conn, list_IntSplit, CONSTANT.sql_IntSplit, CONSTANT.sqld_IntSplit)  # 4.4
#     if len(GetIntCycle) > 0:
#         WriteOriginalData(conn, list_IntCycle, CONSTANT.sql_IntCycle, CONSTANT.sqld_IntCycle)  # 4.5
#     if len(list_RealTimePhase) > 0:
#         # WriteOriginalData(conn, list_StrInput, CONSTANT.sql_StrInput, CONSTANT.sqld_StrInput)  # 4.6
#         WriteOriginalData(conn, list_RealTimePhase, CONSTANT.sql_RealTimePhase, CONSTANT.sqld_RealTimePhase)  # 4.8
#
#
#     return
#

# 向接口请求动态数据
def RequestDynaDataFromInt():
    list_ManoperationRecord = []  # 4.7
    GetManoperationRecord = []
    Now = dt.datetime.now()  # 获取当前时间
    TimeDelta = dt.timedelta(seconds=CONSTANT.TimeDelta)  # 读接口的时间差
    TimeDelay = dt.timedelta(seconds=CONSTANT.TimeDelay)
    StartTime = (Now - TimeDelta - TimeDelay).strftime('%Y-%m-%d %H:%M:%S')
    EndTime = (Now).strftime('%Y-%m-%d %H:%M:%S')
    # StartTime = '2018-11-06 00:00:00'
    # EndTime = '2018-11-06 08:59:00'
    # print(StartTime)

    # print(EndTime)

    payload1 = {r'STime': StartTime, r'ETime': EndTime}
    try:
        # print(payload1)
        get_response = requests.get(r'http://33.83.100.138:8080/getOperatorIntervention.html',
                                             params=payload1)  # 4.7
        GetManoperationRecord = get_response.text
        # print('url = ', get_response.url)
        print("操作记录请求成功")
    except Exception as e:
        print(e)
        # print(GetManoperationRecord)
        # print(GetManoperationRecord[-3])
    if len(GetManoperationRecord) > 0:
        if GetManoperationRecord[-3] != '[':
            list_ManoperationRecord = \
                runManOperation(GetManoperationRecord, list_ManoperationRecord)
        else:
            pass

    if len(list_ManoperationRecord) == 0:
        Log.info("ManoperationRecord no data!")
        print("list_ManoperationRecord接口无数据返回")
    else:
        Log.info("ManoperationRecord get data successfully!num=%s" % len(list_ManoperationRecord))
        # print("list_ManoperationRecord接口获取数据成功！")

    # 原始实时数据和解析数据分别写入数据库
    try:
        conn = cx_Oracle.connect(CONSTANT.OracleUser)
    except Exception as e:
        Log.error(e)
        # print('RequestDynaDataFromInt:',e)
    else:
        if len(list_ManoperationRecord) > 0:
            WriteDynaData(conn, list_ManoperationRecord, CONSTANT.sql_ManoperationRecord)  # 4.7
        conn.commit()
        conn.close()

    return



# 4.7获取人工操作记录

def runManOperation(getmanoperationrecord, list_ManoperationRecord):
    GetManoperationRecord = json.loads(getmanoperationrecord)
    ManoperationRecord = GetManoperationRecord["resultList"]
    # list_ManoperationRecord = []
    if any(ManoperationRecord):
        for i in ManoperationRecord:
            # print(i)
            Oper = i['Oper']
            OperCode = i['OperCode']
            OperTime = i['OperTime']
            OperType = i['OperType']
            Region = i['Region']
            SiteID = i['SiteID']
            Userid = i['Userid']
            # print(type(OperTime))
            itemlist = [Oper, OperCode, OperTime, OperType, Region, SiteID, Userid]
            list_ManoperationRecord.append(itemlist)
    else:
        pass
    return list_ManoperationRecord



def SearchSiteId(row, SiteId):
    Searchrow = []
    for i in row:

        if int(i[9]) == SiteId:
            Searchrow.append(list(i))
        else:
            pass
    return Searchrow


def MainLoop():
    global timer
    global last_date
    global IntStrInput  # 配置表
    timer = threading.Timer(CONSTANT.interval, MainLoop)
    timer.start()
    error_inf = []
    currenttime = dt.datetime.now()
    date = currenttime.date()
    if last_date == date:
        pass
    else:
        last_date = date
    # GetIntInformation, GetFlowInformation, RunStrInformation, GetIntGSIN, GetIntCycle, IntStrInput, \
    # GetManoperationRecord, GetRealTimePhase = RequestDynaDataFromInt(IntersectSiteIDlist)  # 向接口请求动态数据
    error_inf = RequestDynaDataFromInt()  # 向接口请求动态数据并解析


    return


# 列表分组函数
def int_grouped(int_list, one_group):
    num = len(int_list)
    group_num = int(num/one_group)
    group_result = []
    for i in range(group_num):
        if i < group_num-1:
            int_select = int_list[i*one_group:(i+1)*one_group]
        else:
            int_select = int_list[i * one_group:]
        group_result.append(int_select)
    return group_result


def MainProcess():
    global IntersectIDlist  # 路口列表
    global IntStrInput  # 配置表
    global last_date
    IntStrInput = []
    currenttime = dt.datetime.now()
    last_date = currenttime.date()
    try:
        IntersectInfo, conn = CallOracle()  # 从数据库读取路口列表
        IntersectIDlist = IntersectInfo['SITEID']
        one_group = CONSTANT.ONEGROUP
        IntersectIDlist = int_grouped(IntersectIDlist, one_group)
        # print(IntersectIDlist)
        # print(len(IntersectSiteIDlist))
        # RequestHistoryDataFromInt(IntersectIDlist)  # 请求历史数据并写入数据库，返回解析所需配置表
        # RequestHistoryDataFromInt(IntersectIDlist, conn)  # 请求历史数据并写入数据库
        print("conn_success")
    except Exception as e:
        print('IntersectInfo',e)
        # try:
        #     with open('E:\PROGRAME\log\Request_Data_From_Int' + str(last_date) + '.txt', 'r+') as f:
        #         f.read()
        #         f.write('Request_Data_From_Int>' + str(e) +'\n')
        # except Exception as e:
        #     print(e)
        #     with open('E:\PROGRAME\log\Request_Data_From_Int' + str(last_date) + '.txt', 'w') as f:
        #         f.write('Request_Data_From_Int>' + str(e) + '\n')

    else:
        cr = conn.cursor()  # 建立游标
        try:
            cr.execute("SELECT * FROM INT_STR_INPUT order by SITEID")  # 从Oracle中读取数据
            IntStrInput = cr.fetchall()
        except Exception as e:
            print(e)
        finally:
            cr.close()
            conn.close()
        timer = threading.Timer(1, MainLoop)
        timer.start()
    return


if __name__ == '__main__':
    MainProcess()