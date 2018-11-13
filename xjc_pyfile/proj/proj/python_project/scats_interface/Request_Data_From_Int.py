
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
    # StartTime = '2018-10-08 00:00:00'
    # EndTime = '2018-10-08 23:59:00'
    # print(StartTime)
    EndTime = (Now).strftime('%Y-%m-%d %H:%M:%S')
    # print(EndTime)

    payload1 = {r'STime': StartTime, r'ETime': EndTime}
    try:
        # print(payload1)
        get_response = requests.get(r'http://33.83.100.138:8080/getOperatorIntervention.html',
                                             params=payload1)  # 4.7
        GetManoperationRecord = get_response.text
        # print('url = ', get_response.url)
        # print(GetManoperationRecord)
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
        Log.info("list_ManoperationRecord接口无数据返回")
        print("list_ManoperationRecord接口无数据返回")
    else:
        Log.info("list_ManoperationRecord接口获取数据成功！")
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

#
# # 4.1获取路口基础信息
# def runBasicInfo(getintinformation, list_IntInfo):
#     IntInformation = json.loads(getintinformation)
#     IntInfo = IntInformation["resultList"]
#     # list_IntInfo = []
#     if any(IntInfo):
#         for i in IntInfo:
#             Detector = i['Detector']
#             PhaseNo = i['PhaseNo']
#             Region = i['Region']
#             SINo = i['SINo']
#             SiteID = i['SiteID']
#             SiteName = i['SiteName']
#             SubSystemID = i['SubSystemID']
#             itemlist = [Detector, PhaseNo, Region, SINo, SiteID, SiteName, SubSystemID]
#             list_IntInfo.append(itemlist)
#     else:
#         pass
#     return list_IntInfo
#
#
# # 4.2获取流量信息
# def runFlowInfo(getflowinformation, list_FlowInfo):
#     FlowInformation = json.loads(getflowinformation)
#     FlowInfo = FlowInformation["resultList"]
#     # list_FlowInfo = []
#     if any(FlowInfo):
#         for i in FlowInfo:
#             DetectorNo = i['DetectorNo']
#             Flow = i['Flow']
#             FlowTime = i['FlowTime']
#             SiteID = i['SiteID']
#             TimeDistance = i['TimeDistance']
#             itemlist = [DetectorNo, Flow, FlowTime, SiteID, TimeDistance]
#             list_FlowInfo.append(itemlist)
#     else:
#         pass
#     return list_FlowInfo
#
#
# # 4.3获取战略运行记录
# def runStrategicInfo(runstrinformation, list_RunStrInfo, list_RunStrInfoSalkList):
#     RunStrInformation = json.loads(runstrinformation)
#     RunStrInfo = RunStrInformation['resultList']
#     # list_RunStrInfo = []
#     # list_RunStrInfoSalkList = []
#     j = 0
#
#     if any(RunStrInfo):
#         for i in RunStrInfo:
#             A = i['A']
#             ActiveLinkPlan = i['ActiveLinkPlan']
#             ActiveSystemPlan = i['ActiveSystemPlan']
#             ActualCycleTime = i['ActualCycleTime']
#             B = i['B']
#             C = i['C']
#             D = i['D']
#             E = i['E']
#             F = i['F']
#             G = i['G']
#             Id = i['Id']
#             if i['IsSALK'] == 'true':
#                 IsSALK = 1
#             else:
#                 IsSALK = 0
#             NominalCycleTime = i['NominalCycleTime']
#             time_str = i['RecvTime']
#             time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')  # 根据字符串本身的格式进行转换
#             d = time.strftime('%Y-%m-%d')
#             t = time.strftime('%H:%M:%S')
#             RecvDate = d
#             RecvTime = t
#             Region = i['Region']
#             RequiredCycleTime = i['RequiredCycleTime']
#             Saturation = i['Saturation']
#             SiteID_T = i['SiteID']
#             StrategicCycleTime = i['StrategicCycleTime']
#             SubSystemID = i['SubSystemID']
#
#             itemlist1 = [RecvDate, RecvTime, A, ActiveLinkPlan, ActiveSystemPlan, ActualCycleTime, B, C, D, E, F, G, Id,
#                          IsSALK, NominalCycleTime, Region, RequiredCycleTime, Saturation, SiteID_T,
#                          StrategicCycleTime, SubSystemID]
#             list_RunStrInfo.append(itemlist1)
#
#             for z in RunStrInfo[j]['SalkList']:
#                 ADS = z['ADS']
#                 DS1 = z['DS1']
#                 DS2 = z['DS2']
#                 DS3 = z['DS3']
#                 DS4 = z['DS4']
#                 Id = z['Id']
#                 IsSALK = z['IsSALK']
#                 PhaseBitMask = z['PhaseBitMask']
#                 PhaseTime = z['PhaseTime']
#                 SALKNo = z['SALKNo']
#                 SiteID = z['SiteID']
#                 VK1 = z['VO2']
#                 VK2 = z['VK2']
#                 VK3 = z['VK3']
#                 VK4 = z['VK4']
#                 VO1 = z['VO3']
#                 VO2 = z['VO4']
#                 VO3 = z['VK1']
#                 VO4 = z['VO1']
#                 smId = z['smId']
#                 itemlist2 = [RecvDate, RecvTime, SiteID_T, ADS, DS1, DS2, DS3, DS4, Id, IsSALK, PhaseBitMask, PhaseTime,
#                              SALKNo, SiteID, VK1, VK2, VK3, VK4, VO1, VO2, VO3, VO4, smId]
#                 list_RunStrInfoSalkList.append(itemlist2)
#             if j == len(RunStrInfo):
#                 j = 0
#             j = j+1
#     else:
#         pass
#     return list_RunStrInfo, list_RunStrInfoSalkList
#
#
# # 4.4获取路口绿信比
# def runGsign(getintsplit, list_IntSplit):
#     IntGSIN = json.loads(getintsplit)
#     IntSplit = IntGSIN["resultList"]
#     # list_IntSplit = []
#     if any(IntSplit):
#         for i in IntSplit:
#             A = i['A']
#             B = i['B']
#             C = i['C']
#             D = i['D']
#             E = i['E']
#             F = i['F']
#             G = i['G']
#             KeyPhase = i['KeyPhase']
#             LightgroupCount = i['LightgroupCount']
#             PhaseCount = i['PhaseCount']
#             PhaseSort = i['PhaseSort']
#             PhaseTime = i['PhaseTime']
#             PlanNo = i['PlanNo']
#             SiteID = i['SiteID']
#             itemlist = [A, B, C, D, E, F, G, KeyPhase, LightgroupCount, PhaseCount, PhaseSort, PhaseTime, PlanNo, SiteID]
#             list_IntSplit.append(itemlist)
#         # WriteOracle(list_IntSplit, sql_IntSplit)
#     else:
#         pass
#     return list_IntSplit
#
#
# # 4.5获取路口周期
# def runCycle(getintcycle, list_IntCycle):
#     GetIntCycle = json.loads(getintcycle)
#     IntCycle = GetIntCycle["resultList"]
#     # list_IntCycle = []
#     if any(IntCycle):
#         for i in IntCycle:
#             HCL = i['HCL']
#             LCL = i['LCL']
#             REGION = i['Region']
#             SCL1 = i['SCL1']
#             SCL2 = i['SCL2']
#             SZ1 = i['SZ1']
#             SZ2 = i['SZ2']
#             SUBSYSTEMID = i['SubSystemID']
#             XCL = i['XCL']
#             itemlist = [HCL, LCL, REGION, SCL1, SCL2, SZ1, SZ2, SUBSYSTEMID, XCL]
#             list_IntCycle.append(itemlist)
#     else:
#         pass
#     return list_IntCycle
#
#
# # 4.6获取路口战略输入
# def runStategicInput(intstrinput, list_StrInput):
#     IntStrInput = json.loads(intstrinput)
#     StrInput = IntStrInput["resultList"]
#     # list_StrInput = []
#     if any(StrInput):
#         for i in StrInput:
#             Detector = i['Detector']
#             DiretionName = i['DiretionName']
#             Lane1 = i['Lane1']
#             Lane2 = i['Lane2']
#             Lane3 = i['Lane3']
#             Lane4 = i['Lane4']
#             LaneNumber = i['LaneNumber']
#             PhaseNo = i['PhaseNo']
#             SINo = i['SINo']
#             SiteID = i['SiteID']
#             TrafficFlowDir = i['TrafficFlowDir']
#             itemlist = [Detector, DiretionName, Lane1, Lane2, Lane3, Lane4, LaneNumber, PhaseNo, SINo, SiteID, TrafficFlowDir]
#             list_StrInput.append(itemlist)
#     else:
#         pass
#     return list_StrInput


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


# def SecondToTime(s):
#     if s < 60:
#         second = s
#         minutes = 0
#     else:
#         second = s % 60
#         minutes = s/60
#     return '0:%d:%d' % (minutes, second)
#
#
# # 检索出和数据匹配的配置表数据，由于配置表异常所以要判断配置表是否正确
# def SearchPhase(i, DSList):
#     Compdata = []
#     LaneOut = []
#     global CountLaneNo       # 定义全局变量，用来统计与原数据对应的配置表数据条数
#     Lane = (i[2:6])
#     s1 = Lane.count(None)     # 配置表数据中车道数据NULL的数量
#     s2 = DSList.count(None)   # 获得的饱和度数据中NULL的数量
#     for j in range(len(DSList)):
#         s3 = Lane[j] and DSList[j]  # 将两个数据列表的元素分别相与，结果存入Compdata
#         Compdata.append(s3)
#
#     s4 = Compdata.count(None)   # s4为与操作后数据中NULL的个数
#     if s4 == s1 == s2:            # 如果数据位置和数量都对应则返回车道数据，CountLaneNo+1
#         CountLaneNo = CountLaneNo+1
#         LaneOut = (i[2:6])
#         return LaneOut
#     else:
#         return Lane
#
#
# # 根据战略运行记录中的条件换算每个相位的时长
# def PhaseStartTime(N, CycleTime):
#     if N != None:
#         if N <= 80:
#             out = N/100*CycleTime
#         if 81 < N < 127:
#             out = N-80
#         if 128 <= N:
#             out = (N-128)/100*CycleTime
#         return int(out)
#     else:
#         return N
# 根据日期计算星期
# def get_week_day(date):
#     week_day_dict = {
#         0: 'Monday',
#         1: 'Tuesday',
#         2: 'Wednesday ',
#         3: 'Thursday',
#         4: 'Friday',
#         5: 'Saturday',
#         6: 'Sunday',
#     }
#     day = date.weekday()
#     return week_day_dict[day]


# 检索出对应路口号数据
def SearchSiteId(row, SiteId):
    Searchrow = []
    for i in row:

        if int(i[9]) == SiteId:
            Searchrow.append(list(i))
        else:
            pass
    return Searchrow


# 返回timedelta格式的相位时长，用来计算时间的加减
def PhaseTimeOut(Phase, ActualCycleTime):
    import datetime
    Change = PhaseStartTime(Phase, ActualCycleTime)
    StrDate = datetime.timedelta(seconds=Change)
    return StrDate

# # 解析程序
# def SkitListOut(runstrinformation, row, list_AnalyzedRunInfo):
#     RunStrInformation = json.loads(runstrinformation)
#     RunStrInfo = RunStrInformation['resultList']
#
#     global CountLaneNo
#     # global insert_list1
#     # global insert_list2
#     global R_cycle_time
#
#     # insert_list1 = []
#     # insert_list2 = []
#     j = 0
#
#     for i in RunStrInfo:      # 遍历resultList中的数据元素
#         A = i['A']
#         ActiveLinkPlan = i['ActiveLinkPlan']
#         ActiveSystemPlan = i['ActiveSystemPlan']
#         ActualCycleTime = i['ActualCycleTime']
#         B = i['B']
#         C = i['C']
#         D = i['D']
#         E = i['E']
#         F = i['F']
#         G = i['G']
#         Id = i['Id']
#         if i['IsSALK'] == 'true':
#             IsSALK = 1
#         else:
#             IsSALK = 0
#         NominalCycleTime = i['NominalCycleTime']
#         RecvTime = i['RecvTime']
#         Region = i['Region']
#         RequiredCycleTime = i['RequiredCycleTime']
#         Saturation = i['Saturation']
#         SiteID_T = i['SiteID']
#         StrategicCycleTime = i['StrategicCycleTime']
#         SubSystemID = i['SubSystemID']
#
#         cycletime = SecondToTime(ActualCycleTime)                      # 秒转换格式
#         DATE_RECVTIME = datetime.strptime(RecvTime[:10], '%Y-%m-%d')    # time转换成datetime
#         END_REVCTIME = datetime.strptime(RecvTime[11:], '%H:%M:%S')
#         cycletime_C = datetime.strptime(cycletime, '%H:%M:%S')
#         R_cycle_time = END_REVCTIME - cycletime_C+DATE_RECVTIME       # 计算周期开始时间
#         R_cycle_time1 = str(R_cycle_time)       # 强制转换字符串格式
#
#         FstrCycleStartTime = R_cycle_time1[11:]   # 获取时分秒部分
#
#         PhaseList = [A, B, C, D, E, F, G]
#         PhaseBit = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
#         PhaseTimeList = dict(zip(PhaseBit, PhaseList))  # 创建字典将相位和各相位的数据组合
#
#         number = 0
#         PhaseStartTimeList = []
#         StrPhaseStartTime = []
#         PhaseTime_A = R_cycle_time      # A相位开始时间即周期开始时间
#         PhaseStartTimeList.append(PhaseTime_A)       # 存入datetime格式的A相位开始时间用来计算
#         StrPhaseStartTime.append(R_cycle_time1[11:])  # 存入字符串格式的A相位开始时间用来输出
#         RealPhase = []
#         StrFstrDate = []
#         for q in PhaseBit:             # 遍历相位列表计算各相位开始时间，并存入StrPhaseStartTime
#             if PhaseTimeList[q] != None:
#                 RealPhase.append(q)
#                 PhaseDate = PhaseTimeOut(PhaseTimeList[q], ActualCycleTime)
#                 DateStartTime = PhaseStartTimeList[number] + PhaseDate
#                 StrPhaseDate = str(DateStartTime)[11:]
#                 FstrDate = str(DateStartTime)[:10]
#                 StrPhaseStartTime.append(StrPhaseDate)
#                 StrFstrDate.append(FstrDate)
#                 PhaseStartTimeList.append(DateStartTime)
#                 number += 1
#             else:
#                 pass
#         DictPhaseTime = dict(zip(RealPhase, StrPhaseStartTime))   # 创建字典来保存相位和对应的开始时间
#         DictFstrDate = dict(zip(RealPhase, StrFstrDate))
#
#         # itemlist = [A, ActiveLinkPlan, ActiveSystemPlan, ActualCycleTime, B, C, D, E, F, G, Id, IsSALK, NominalCycleTime,
#         #         RecvTime, Region, RequiredCycleTime, Saturation, SiteID_T, StrategicCycleTime, SubSystemID]
#         # list_AnalyzedRunInfo.append(itemlist)  # 表1
#
#         SearchLane1 = SearchSiteId(row, SiteID_T)                # 检索配置表对应路口号，返回对应数据
#         for z in RunStrInfo[j]['SalkList']:     # 遍历SalkList
#             ADS = z['ADS']
#             DS1 = z['DS1']
#             DS2 = z['DS2']
#             DS3 = z['DS3']
#             DS4 = z['DS4']
#             Id = z['Id']
#             IsSALK = z['IsSALK']
#             PhaseBitMask = z['PhaseBitMask']
#             PhaseTime = z['PhaseTime']
#             SALKNo = z['SALKNo']
#             SiteID = z['SiteID']
#             VK1 = z['VO2']
#             VK2 = z['VK2']
#             VK3 = z['VK3']
#             VK4 = z['VK4']
#             VO1 = z['VO3']
#             VO2 = z['VO4']
#             VO3 = z['VK1']
#             VO4 = z['VO1']
#             smId = z['smId']
#             DSList = [DS1, DS2, DS3, DS4]
#             VOList = [VO1, VO2, VO3, VO4]
#             OutPhaseTime = DictPhaseTime[PhaseBitMask[0]]   # 找出对应相位的开始时间
#             OutFstrDate = DictFstrDate[PhaseBitMask[0]]
#             DATE_FstrDate = datetime.strptime(OutFstrDate, '%Y-%m-%d') # 日期转datetime格式
#
#             WeekDay = get_week_day(DATE_FstrDate)           # 获取对应的星期
#             CountLaneNo = 1
#             ConfigVersion = ''
#             if SiteID_T != SiteID:
#                 SearchLane = SearchSiteId(row, SiteID)
#             else:
#                 SearchLane = SearchLane1
#             try:
#                 if IsSALK == 0:     # 如果IsSalk为0，将数据存入LIST
#                     CountLaneNo = 0
#                     for m in SearchLane:
#                         if PhaseBitMask == m[7] and SALKNo == int(m[8]):
#                             LaneNo = SearchPhase(m, DSList)
#                             for n in range(4):
#                                 DS = DSList[n]
#                                 VO = VOList[n]
#                                 #print(LaneNo)
#                                 if LaneNo[n] != None and CountLaneNo == 1:        # 车道不是NULL则输出
#                                     itemlist = [SiteID, SALKNo, LaneNo[n], FstrCycleStartTime, OutPhaseTime, PhaseBitMask, PhaseTime, ActualCycleTime,DS, VO, OutFstrDate, WeekDay, ConfigVersion]
#                                     list_AnalyzedRunInfo.append(itemlist)
#                                     print('路口：' + str(SiteID) + ' 日期：' + str(OutFstrDate) + ' 时间：' + str(FstrCycleStartTime) + '配置正确')
#                                 else:
#                                     pass
#                         else:
#                             pass
#                     if CountLaneNo == 0:
#                         print('路口：' + str(SiteID) + ' 日期：' + str(OutFstrDate) + ' 时间：' + str(FstrCycleStartTime))
#                         print('未找到对应配置表，请检查配置表是否正确')
#                     if CountLaneNo >= 2:
#                         print('路口：' + str(SiteID) + ' 日期：' + str(OutFstrDate) + ' 时间：' + str(FstrCycleStartTime))
#                         print('匹配多行配置表数据，请检查配置表是否正确')
#                 else:
#                     pass
#             except:
#                 print('配置表发生未知错误')
#
#         if j == len(RunStrInfo):
#             j = 0
#         j = j+1
#     return list_AnalyzedRunInfo


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
    error_inf = RequestDynaDataFromInt(IntersectIDlist, IntStrInput)  # 向接口请求动态数据并解析


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
        try:
            with open('E:\PROGRAME\log\Request_Data_From_Int' + str(last_date) + '.txt', 'r+') as f:
                f.read()
                f.write('Request_Data_From_Int>' + str(e) +'\n')
        except Exception as e:
            print(e)
            with open('E:\PROGRAME\log\Request_Data_From_Int' + str(last_date) + '.txt', 'w') as f:
                f.write('Request_Data_From_Int>' + str(e) + '\n')

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