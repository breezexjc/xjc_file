import os
import sys
import requests
import datetime as dt
import cx_Oracle
import threading
import json
import time
from datetime import datetime
import numpy as np
import psycopg2 as pg
import pandas as pd
import logging
from .pram_conf import CONSTANT


Log = logging.getLogger('scats')  # 获取settings.py配置文件中logger名称

class PGCONN(object):
    ip = '33.83.100.145'
    database = 'signal_specialist'
    port = 5432
    account = 'postgres'
    password = 'postgres'
    pg_conn = []
    def __init__(self):
        self.ip = PGCONN.ip
        self.database = PGCONN.database
        self.port = PGCONN.port
        self.account = PGCONN.account
        self.password = PGCONN.password
        self.conn = PGCONN.pg_conn
        self.cr = []

    def connect(self):
        try:
            self.conn = pg.connect(database=self.database,user=self.account,password=self.password,
                                    host=self.ip,
                                    port=self.port)
            self.cr = self.conn.cursor()
        except Exception as e:
            print(self.ip +"connect failed")
            return self.conn, self.cr
        else:
            print(self.ip +"connect succeed")

            return self.conn, self.cr

    def close(self):
        self.cr.close()
        self.conn.close()


def SecondToTime(s):
    if s < 60:
        second = s
        minutes = 0
    else:
        second = s % 60
        minutes = s/60
    return '0:%d:%d' % (minutes, second)


# 检索出对应路口号数据
def SearchSiteId(row, SiteId):
    Searchrow = []
    for i in row:

        if int(i[9]) == SiteId:
            Searchrow.append(list(i))
        else:
            pass
    return Searchrow


def phase_ratio_cal(N, CycleTime):
    out = 0
    if N != None:
        if N <= 80:
            out = N
        if 81 < N < 127:
            out = (N-80)*100/CycleTime
        if 128 <= N:
            out = (N-128)
        return int(out)
    else:
        return N


# 根据战略运行记录中的条件换算每个相位的时长
def PhaseStartTime(N, CycleTime):
    out = 0
    if N != None:
        if N <= 80:
            out = N/100*CycleTime
        if 81 < N < 127:
            out = N-80
        if 128 <= N:
            out = (N-128)/100*CycleTime
        return int(out)
    else:
        return N


# 返回timedelta格式的相位时长，用来计算时间的加减
def PhaseTimeOut(Phase, ActualCycleTime):
    import datetime
    Change = PhaseStartTime(Phase, ActualCycleTime)
    StrDate = datetime.timedelta(seconds=Change)
    return StrDate


# 根据日期计算星期
def get_week_day(date):
    week_day_dict = {
        0: 'Monday',
        1: 'Tuesday',
        2: 'Wednesday ',
        3: 'Thursday',
        4: 'Friday',
        5: 'Saturday',
        6: 'Sunday',
    }
    day = date.weekday()
    return week_day_dict[day]


# 连接数据库读取路口list
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
            cr.close()
            db.close()
        except cx_Oracle.DatabaseError:
            print('ERROR:数据表名输入错误或不存在')
            # sys.exit(0)
        else:
            match_records = pd.DataFrame(rs1)
            # print(match_records)
            match_records.columns = ['SITEID', 'SITENAME', 'REGION']

    return match_records


# 检索出和数据匹配的配置表数据，由于配置表异常所以要判断配置表是否正确
def SearchPhase(i, DSList):
    Compdata = []
    LaneOut = []
    global CountLaneNo       # 定义全局变量，用来统计与原数据对应的配置表数据条数
    Lane = (i[2:6])
    s1 = Lane.count(None)     # 配置表数据中车道数据NULL的数量
    s2 = DSList.count(None)   # 获得的饱和度数据中NULL的数量
    for j in range(len(DSList)):
        s3 = Lane[j] and DSList[j]  # 将两个数据列表的元素分别相与，结果存入Compdata
        Compdata.append(s3)

    s4 = Compdata.count(None)   # s4为与操作后数据中NULL的个数
    if s4 == s1 == s2:            # 如果数据位置和数量都对应则返回车道数据，CountLaneNo+1
        CountLaneNo = CountLaneNo+1
        LaneOut = (i[2:6])
        return LaneOut
    else:
        return Lane


def int_grouped(node_num, group):
    node_list = []
    for i in range(int(group)):
        try:
            select_node = node_num[i*CONSTANT.group_interval:(i+1)*CONSTANT.group_interval]
        except Exception as e:
            select_node = node_num[i*CONSTANT.group_interval:]
            # select_node = node_num[i*100:]
            print(e)
        # print(select_node)
        node_list.append(select_node)
    # print(node_list)
    return node_list


# 4.3获取战略运行记录
def runStrategicInfo(runstrinformation, list_RunStrInfo, list_RunStrInfoSalkList):
    RunStrInformation = json.loads(runstrinformation)
    RunStrInfo = RunStrInformation['resultList']
    # list_RunStrInfo = []
    # list_RunStrInfoSalkList = []
    j = 0
    if any(RunStrInfo):
        for i in RunStrInfo:
            A = i['A']
            ActiveLinkPlan = i['ActiveLinkPlan']
            ActiveSystemPlan = i['ActiveSystemPlan']
            ActualCycleTime = i['ActualCycleTime']
            B = i['B']
            C = i['C']
            D = i['D']
            E = i['E']
            F = i['F']
            G = i['G']
            Id = i['Id']
            if i['IsSALK'] == 'true':
                IsSALK = 1
            else:
                IsSALK = 0
            NominalCycleTime = i['NominalCycleTime']
            time_str = i['RecvTime']
            time = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')  # 根据字符串本身的格式进行转换
            d = time.strftime('%Y-%m-%d')
            t = time.strftime('%H:%M:%S')
            RecvDate = d
            RecvTime = t
            Region = i['Region']
            RequiredCycleTime = i['RequiredCycleTime']
            Saturation = i['Saturation']
            SiteID_T = i['SiteID']
            StrategicCycleTime = i['StrategicCycleTime']
            SubSystemID = i['SubSystemID']

            itemlist1 = [RecvDate, RecvTime, A, ActiveLinkPlan, ActiveSystemPlan, ActualCycleTime, B, C, D, E, F, G, Id,
                         IsSALK, NominalCycleTime, Region, RequiredCycleTime, Saturation, SiteID_T,
                         StrategicCycleTime, SubSystemID]
            list_RunStrInfo.append(itemlist1)

            for z in RunStrInfo[j]['SalkList']:
                ADS = z['ADS']
                DS1 = z['DS1']
                DS2 = z['DS2']
                DS3 = z['DS3']
                DS4 = z['DS4']
                Id = z['Id']
                IsSALK = z['IsSALK']
                PhaseBitMask = z['PhaseBitMask']
                PhaseTime = z['PhaseTime']
                SALKNo = z['SALKNo']
                SiteID = z['SiteID']
                VK1 = z['VO2']
                VK2 = z['VK2']
                VK3 = z['VK3']
                VK4 = z['VK4']
                VO1 = z['VO3']
                VO2 = z['VO4']
                VO3 = z['VK1']
                VO4 = z['VO1']
                smId = z['smId']
                itemlist2 = [RecvDate, RecvTime, SiteID_T, ADS, DS1, DS2, DS3, DS4, Id, IsSALK, PhaseBitMask, PhaseTime,
                             SALKNo, SiteID, VK1, VK2, VK3, VK4, VO1, VO2, VO3, VO4, smId]
                list_RunStrInfoSalkList.append(itemlist2)
            if j == len(RunStrInfo):
                j = 0
            j = j+1
    else:
        pass
    return list_RunStrInfo, list_RunStrInfoSalkList


# 动态数据写入数据库
def WriteDynaData(conn, itemlist, sql):  # 原始数据写入数据库
    cursor = conn.cursor()
    for i in itemlist:
        if len(i):
            try:
                cursor.execute(sql, i)
            except cx_Oracle.IntegrityError as e:
                conn.commit()
            except Exception as e:
                print(e,i)
                pass
            conn.commit()
    cursor.close()
    return


# 解析程序
def SkitListOut(runstrinformation, row, list_AnalyzedRunInfo):
    RunStrInformation = json.loads(runstrinformation)
    RunStrInfo = RunStrInformation['resultList']
    currenttime = dt.datetime.now()
    date = currenttime.date()
    global CountLaneNo
    global R_cycle_time
    j = 0

    for i in RunStrInfo:      # 遍历resultList中的数据元素
        A = i['A']
        ActiveLinkPlan = i['ActiveLinkPlan']
        ActiveSystemPlan = i['ActiveSystemPlan']
        ActualCycleTime = i['ActualCycleTime']
        B = i['B']
        C = i['C']
        D = i['D']
        E = i['E']
        F = i['F']
        G = i['G']
        Id = i['Id']
        if i['IsSALK'] == 'true':
            IsSALK = 1
        else:
            IsSALK = 0
        NominalCycleTime = i['NominalCycleTime']
        RecvTime = i['RecvTime']
        Region = i['Region']
        RequiredCycleTime = i['RequiredCycleTime']
        Saturation = i['Saturation']
        SiteID_T = i['SiteID']
        StrategicCycleTime = i['StrategicCycleTime']
        SubSystemID = i['SubSystemID']

        cycletime = SecondToTime(ActualCycleTime)                      # 秒转换格式
        DATE_RECVTIME = datetime.strptime(RecvTime[:10], '%Y-%m-%d')    # time转换成datetime
        END_REVCTIME = datetime.strptime(RecvTime[11:], '%H:%M:%S')
        cycletime_C = datetime.strptime(cycletime, '%H:%M:%S')
        R_cycle_time = END_REVCTIME - cycletime_C+DATE_RECVTIME       # 计算周期开始时间
        R_cycle_time1 = str(R_cycle_time)       # 强制转换字符串格式

        FstrCycleStartTime = R_cycle_time1[11:]   # 获取时分秒部分
        DATE_RECVTIME = datetime.strptime(R_cycle_time1[:10], '%Y-%m-%d')
        # print(R_cycle_time1[:10])
        PhaseList = [A, B, C, D, E, F, G]
        PhaseBit = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
        PhaseTimeList = dict(zip(PhaseBit, PhaseList))  # 创建字典将相位和各相位的数据组合

        number = 0
        PhaseStartTimeList = []
        StrPhaseStartTime = []
        PhaseTime_A = R_cycle_time      # A相位开始时间即周期开始时间
        PhaseStartTimeList.append(PhaseTime_A)       # 存入datetime格式的A相位开始时间用来计算
        StrPhaseStartTime.append(R_cycle_time1[11:])  # 存入字符串格式的A相位开始时间用来输出
        RealPhase = []
        StrFstrDate = []
        for q in PhaseBit:             # 遍历相位列表计算各相位开始时间，并存入StrPhaseStartTime
            if PhaseTimeList[q] != None:
                RealPhase.append(q)
                PhaseDate = PhaseTimeOut(PhaseTimeList[q], ActualCycleTime)
                DateStartTime = PhaseStartTimeList[number] + PhaseDate
                StrPhaseDate = str(DateStartTime)[11:]
                FstrDate = str(DateStartTime)[:10]
                StrPhaseStartTime.append(StrPhaseDate)
                StrFstrDate.append(FstrDate)
                PhaseStartTimeList.append(DateStartTime)
                number += 1
            else:
                pass
        DictPhaseTime = dict(zip(RealPhase, StrPhaseStartTime))   # 创建字典来保存相位和对应的开始时间
        DictFstrDate = dict(zip(RealPhase, StrFstrDate))

        # itemlist = [A, ActiveLinkPlan, ActiveSystemPlan, ActualCycleTime, B, C, D, E, F, G, Id, IsSALK, NominalCycleTime,
        #         RecvTime, Region, RequiredCycleTime, Saturation, SiteID_T, StrategicCycleTime, SubSystemID]
        # list_AnalyzedRunInfo.append(itemlist)  # 表1

        SearchLane1 = SearchSiteId(row, SiteID_T)                # 检索配置表对应路口号，返回对应数据
        for z in RunStrInfo[j]['SalkList']:     # 遍历SalkList

            ADS = z['ADS']
            DS1 = z['DS1']
            DS2 = z['DS2']
            DS3 = z['DS3']
            DS4 = z['DS4']
            Id = z['Id']
            IsSALK = z['IsSALK']
            PhaseBitMask = z['PhaseBitMask']
            PhaseTime = z['PhaseTime']
            SALKNo = z['SALKNo']
            SiteID = z['SiteID']
            VK1 = z['VO2']
            VK2 = z['VK2']
            VK3 = z['VK3']
            VK4 = z['VK4']

            VO1 = z['VO3']
            VO2 = z['VO4']
            VO3 = z['VK1']
            VO4 = z['VO1']
            smId = z['smId']
            DSList = [DS1, DS2, DS3, DS4]
            VOList = [VO1, VO2, VO3, VO4]

            # try:
            #     OutPhaseTime = DictPhaseTime[PhaseBitMask[0]]   # 找出对应相位的开始时间
            #     # OutFstrDate = DictFstrDate[PhaseBitMask[0]]
            # except Exception as e:
            #     print(str(SiteID)+'相位时长有误' ,e)
            #     OutPhaseTime = None
            #     continue
            OutPhaseTime = None
            # print(DictFstrDate)
            # OutFstrDate = DictFstrDate[PhaseBitMask[0]]
            # DATE_FstrDate = datetime.strptime(OutFstrDate, '%Y-%m-%d') # 日期转datetime格式
            DATE_FstrDate = DATE_RECVTIME
            WeekDay = get_week_day(DATE_RECVTIME)           # 获取对应的星期
            CountLaneNo = 1
            ConfigVersion = ''
            if SiteID_T != SiteID:
                SearchLane = SearchSiteId(row, SiteID)
            else:
                SearchLane = SearchLane1
            try:
                if IsSALK == 0:     # 如果IsSalk为0，将数据存入LIST
                    CountLaneNo = 0
                    for m in SearchLane:
                        if PhaseBitMask == m[7] and SALKNo == int(m[8]):
                            LaneNo = SearchPhase(m, DSList)
                            for n in range(4):
                                DS = DSList[n]
                                VO = VOList[n]
                                #print(LaneNo)
                                if LaneNo[n] != None and CountLaneNo == 1:        # 车道不是NULL则输出
                                    itemlist = [SiteID, SALKNo, LaneNo[n], FstrCycleStartTime, OutPhaseTime,
                                                PhaseBitMask, PhaseTime, ActualCycleTime,DS, VO, DATE_FstrDate,
                                                WeekDay, ConfigVersion]
                                    list_AnalyzedRunInfo.append(itemlist)
                                    # print('路口：' + str(SiteID) + ' 日期：' + str(OutFstrDate) + ' 时间：' + str(FstrCycleStartTime) + '配置正确')
                                else:
                                    pass
                        else:
                            pass
                if CONSTANT.IF_CHECK:
                    if CountLaneNo == 0:
                        # print('路口：' + str(SiteID) + '相位' + PhaseBitMask + ' 日期：' + str(OutFstrDate) + ' 时间：' + str(FstrCycleStartTime))
                        # print('未找到对应配置表，请检查配置表是否正确')
                        if [str(SiteID), PhaseBitMask] not in resolve_fail_node:
                            resolve_fail_node.append([str(SiteID), PhaseBitMask])
                            lock.acquire()
                            try:
                                f = open('E:\PROGRAME\log\scats_strategic_resolve_fail_node' + str(date) + '.txt', 'r+')
                                f.read()
                                f.write('路口：' + str(SiteID) + '相位' + PhaseBitMask + '\n')
                                f.write('未找到对应配置表，请检查配置表是否正确' + '\n')
                                f.close()
                            except FileNotFoundError:
                                f = open('E:\PROGRAME\log\scats_strategic_resolve_fail_node' + str(date) + '.txt', 'w')
                                f.write('路口：' + str(SiteID) + '相位' + PhaseBitMask + '\n')
                                f.write('未找到对应配置表，请检查配置表是否正确' + '\n')
                                f.close()
                            finally:
                                lock.release()
                    if CountLaneNo >= 2:
                        # print('路口：' + str(SiteID) + '相位' + PhaseBitMask +' 日期：' + str(OutFstrDate) + ' 时间：' + str(FstrCycleStartTime))
                        # print('匹配多行配置表数据，请检查配置表是否正确')

                        if [str(SiteID), PhaseBitMask] not in resolve_fail_node:
                            resolve_fail_node.append([str(SiteID), PhaseBitMask])
                            lock.acquire()
                            try:
                                f = open('E:\PROGRAME\log\scats_strategic_resolve_fail_node' + str(date) + '.txt', 'r+')
                                f.read()
                                f.write('路口：' + str(SiteID) + '相位' + PhaseBitMask + '\n')
                                f.write('匹配多行配置表数据，请检查配置表是否正确' + '\n')
                                f.close()
                            except FileNotFoundError:
                                f = open('E:\PROGRAME\log\scats_strategic_resolve_fail_node' + str(date) + '.txt', 'w')
                                f.write('路口：' + str(SiteID) + '相位' + PhaseBitMask + '\n')
                                f.write('匹配多行配置表数据，请检查配置表是否正确' + '\n')
                                f.close()
                            finally:
                                lock.release()

                else:
                    pass
            except:
                print('配置表发生未知错误')

        if j == len(RunStrInfo):
            j = 0
        j = j+1
    return list_AnalyzedRunInfo


# 向接口请求动态数据
def RequestDynaDataFromInt(siteIDlist, IntStrInput,n):

    error_inf = []
    currenttime = dt.datetime.now()
    date =currenttime.date()
    list_FlowInfo = []  # 4.2
    list_RunStrInfo = []  # 4.3
    list_RunStrInfoSalkList = []
    list_AnalyzedRunInfo = []  # 解析结果
    list_AnalyzedRunInfoSalkList = []
    RunStrInformation = []
    Now = dt.datetime.now()  # 获取当前时间
    TimeDelta = dt.timedelta(seconds=CONSTANT.TimeDelta)  # 读接口的时间差
    # TempStartTime = Now - TimeDelta
    # TempEndTime = Now
    TimeDelay = dt.timedelta(seconds=CONSTANT.TimeDelay)
    StartTime = (Now - TimeDelta - TimeDelay).strftime('%Y-%m-%d %H:%M:%S')
    EndTime = (Now - TimeDelay).strftime('%Y-%m-%d %H:%M:%S')
    if CONSTANT.IF_DATA_REPAIR:
        StartTime = CONSTANT.S_REPAIR_DATE
        EndTime = CONSTANT.E_REPAIR_DATE

    scuuess_request = 0
    for m in range(len(siteIDlist)):
        SiteID = siteIDlist[m]
        payload1 = {'SiteID': SiteID, 'STime': StartTime, 'ETime': EndTime}
        # 4.3获取战略运行记录
        try:
            RunStrInformation = requests.get(r'http://33.83.100.138:8080/getStrategicmonitor.html',
                                             params=payload1,timeout=5)  # 4.3
            RunStrInformation = RunStrInformation.text
            # print(RunStrInformation[-50:])
        except Exception as e:
            print(e)
            Log.warning("RunStrInformation: request timeout!")
        else:
            scuuess_request += 1

        if len(RunStrInformation) > 0:
            if RunStrInformation[-3] != '[':
                # enjoy_strategicInfo.append(RunStrInformation)
                # print(enjoy_strategicInfo)
                list_RunStrInfo, list_RunStrInfoSalkList = \
                    runStrategicInfo(RunStrInformation, list_RunStrInfo, list_RunStrInfoSalkList)
                if SiteID in siteIDlist:
                    # print('存在配置表')
                    list_AnalyzedRunInfo = \
                    SkitListOut(RunStrInformation, IntStrInput, list_AnalyzedRunInfo)
            else:
                key = no_data_node.keys()
                if str(SiteID) not in key:
                    no_data_node[str(SiteID)] = 1
                elif str(SiteID) in key:
                    no_data_node[str(SiteID)] += 1
                else:
                    pass

    # Log.info('thread:%s successfully request int num=%s' % (n, scuuess_request))
    if len(list_AnalyzedRunInfo) > 0:
        Log.info('thread:%s receive salklist data successfully!num=%s' % (n, len(list_AnalyzedRunInfo)))
        print('thread:', n, 'scats接口数据接收完毕,')
    elif len(list_AnalyzedRunInfo) == 0:
        Log.info('thread:%s no salklist data received!' % n )
        print("scats接口获取数据失败")
    lock.acquire()
    try:
        real_phase_split_send(list_RunStrInfo,n)
        scats_salk_data_send(list_RunStrInfo, list_RunStrInfoSalkList, list_AnalyzedRunInfo, n)
    finally:
        lock.release()
    return error_inf

# def test(int_grouped, IntStrInput, n):
#     print(n)
#     time.sleep(3)

def thread_creat(group_num, int_grouped,IntStrInput):
    print('thread_creat',group_num)
    try:
        currenttime = dt.datetime.now()
        date = currenttime.date()
        threads = []
        if not CONSTANT.IF_DATA_REPAIR:
            print('current time: %s' % str(dt.datetime.now()))
            try:
                for i in range(int(group_num)):
                    name = 'thread' + str(i)
                    # locals()[name] = i
                    # timer = threading.Timer(1.0, thread_creat, args=[group_num, int_grouped])
                    # timer.start()
                    locals()[name] = threading.Thread(target=RequestDynaDataFromInt, name='get_data',
                                                      args=[int_grouped[i], IntStrInput, i])

                    # locals()[name] = threading.Thread(target=test,
                    #                                   args=[int_grouped[i], IntStrInput, i])
                    print('thread %s is running...' % i)
                    locals()[name].start()
                    threads.append(locals()[name])
                    # print(threading.current_thread().name)
                    # n += 1
                    # thread.join()
                for t in threads:
                    t.join()
                print('all_over')
            except Exception as e:
                print('thread:', e)
            else:
                # print(currenttime)
                keys = no_data_node.keys()
                # with open('E:\PROGRAME\log\scats_strategic_nodata_node' + str(date) + '.txt', 'w+') as f:
                #     # with open('E:\scats_strategic_nodata_node' + str(date) + '.txt', 'w') as f:
                #     for j in keys:
                #         f.write('节点' + j + ':' + str(no_data_node[j]) + '\n')
        else:
            S_REPAIR_DATE = CONSTANT.S_REPAIR_DATE
            E_REPAIR_DATE = CONSTANT.E_REPAIR_DATE
            date_range = pd.date_range(S_REPAIR_DATE, E_REPAIR_DATE, freq=CONSTANT.REQUEST_INTERVAL).tolist()

            for j in range(len(date_range)):
                try:
                    print(CONSTANT.E_REPAIR_DATE)
                    CONSTANT.S_REPAIR_DATE = str(date_range[j])
                    CONSTANT.E_REPAIR_DATE = str(date_range[j+1])
                    print(CONSTANT.E_REPAIR_DATE)
                except Exception as e:
                    print(e)
                else:
                    for i in range(group_num):
                        name = 'thread' + str(i)
                        # locals()[name] = i
                        # timer = threading.Timer(1.0, thread_creat, args=[group_num, int_grouped])
                        # timer.start()
                        locals()[name] = threading.Thread(target=RequestDynaDataFromInt, name='get_data',
                                                          args=[int_grouped[i], IntStrInput, i])

                        # locals()[name] = threading.Thread(target=test,
                        #                                   args=[int_grouped[i], IntStrInput, i])
                        print('thread %s is running...' % i)
                        locals()[name].start()
                        threads.append(locals()[name])
                        # print(threading.current_thread().name)
                        # n += 1
                        # thread.join()
                    for t in threads:
                        t.join()
                    print('all_over')
    except Exception as e:
        print(e)


def real_phase_split_send(list_RunStrInfo,n):
    try:
        pgdatabase = PGCONN()
        pg_conn, cr = pgdatabase.connect()
    except Exception as e:
        print("获取数据成功", e)
    else:
        length_int = len(list_RunStrInfo)
        if pg_conn and length_int > 0:
            for i in list_RunStrInfo:
                # [RecvDate, RecvTime, A, ActiveLinkPlan, ActiveSystemPlan, ActualCycleTime, B, C, D, E, F, G, Id,
                #                          IsSALK, NominalCycleTime, Region, RequiredCycleTime, Saturation, SiteID_T,
                #                          StrategicCycleTime, SubSystemID]
                try:
                    phase_dict = {'A': i[2], 'B': i[6], 'C': i[7], 'D': i[8], 'E': i[9], 'F': i[10], 'G': i[11]}
                    keys = phase_dict.keys()
                    # print(phase_dict)
                    cycle_time = i[5]
                    scats_id = i[18]
                    recv_time = dt.datetime.strptime(i[0] + ' ' + i[1], "%Y-%m-%d %H:%M:%S")
                    if cycle_time is not None and int(cycle_time) > 0:
                        for j in keys:
                            # if phase_dict[j] != None and phase_dict[j] != 1:
                            if phase_dict[j] != None:# and phase_dict[j] != 1:
                                try:
                                    split = phase_ratio_cal(phase_dict[j], cycle_time)
                                    phase_name = j
                                    order_num = None
                                    # phase_result.append([j, phase_ratio_cal(phase_dict[j], cycle_time)])
                                    ratio_data = [scats_id, phase_name, split, order_num, recv_time, cycle_time]

                                    cr.execute(
                                        "insert into recom_compute_reference(scats_id,phase_name,split,order_num,"
                                        "create_time,cycle_length) values(%s,%s,%s,%s,%s,%s) ", ratio_data)
                                except Exception as e:
                                    # print(e)
                                    # print(ratio_data)
                                    # print("here is :", sys._getframe().f_lineno, e)
                                    pass
                                pg_conn.commit()
                except pg.IntegrityError:
                    pg_conn.commit()
                    print("数据重复插入")
                except Exception as e:
                    pg_conn.commit()
                    print("异常错误：", e)
            pgdatabase.close()
            print('thread:', n, 'phase_split_reference插入完毕')
        else:
            print("无战略数据或数据库连接失败")


def scats_salk_data_send(list_RunStrInfo, list_RunStrInfoSalkList, list_AnalyzedRunInfo, n):

    try:
        conn = cx_Oracle.connect(CONSTANT.OracleUser)
    except Exception as e:
        print('WriteDynaData', e)
    else:
        if len(list_RunStrInfo) > 0:
            WriteDynaData(conn, list_RunStrInfo, CONSTANT.sql_RunStrInfo)  # 4.3
            Log.info('thread:', n, 'list_RunStrInfo insert successfully!')
        print('thread:', n, "list_RunStrInfo写入完毕")
        if len(list_RunStrInfoSalkList) > 0:
            WriteDynaData(conn, list_RunStrInfoSalkList, CONSTANT.sql_RunStrInfoSalkList)
            Log.info('thread:', n, 'list_RunStrInfoSalkList insert successfully!')
        print('thread:', n, "list_RunStrInfoSalkList写入完毕")
        if len(list_AnalyzedRunInfo) > 0:
            WriteDynaData(conn, list_AnalyzedRunInfo, CONSTANT.sql_AnalyzedRunInfo)  # 解析结果
            Log.info('thread:', n, 'list_AnalyzedRunInfo insert successfully!')
        print('thread:', n, "list_AnalyzedRunInfo写入完毕")
        conn.commit()
        conn.close()
        # lock.release()
        print('thread:', n, '解析结果传输完毕')
    finally:
        pass


def MainProcess():
    global IntersectIDlist  # 路口列表
    global IntStrInput  # 配置表
    # global enjoy_strategicInfo
    global last_date
    global no_data_node
    global resolve_fail_node
    no_data_node = {}
    resolve_fail_node = []
    IntersectInfo = CallOracle()  # 从数据库读取路口列表
    currenttime = dt.datetime.now()
    last_date = currenttime.date()
    # try:
    #     os.remove('E:\PROGRAME\log\scats_strategic_resolve_fail_node' + str(last_date) + '.txt')
    #     print('清理已存在日志文件')
    # except Exception as e:
    #     print('日志文件还未创建')
    # try:
    #     os.remove('E:\PROGRAME\log\scats_strategic_nodata_node' + str(last_date) + '.txt')
    #     print('清理已存在日志文件')
    # except Exception as e:
    #     print('日志文件还未创建')
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
                cr.close()
                conn.commit()
                conn.close()
                # print(IntersectIDlist)
                # print(group)
                int_id = np.array(IntersectIDlist).tolist()
                int_num = len(int_id)
                print("请求总路口数：", int_num)
                group = round(int_num / CONSTANT.group_interval, 0)+1
                int_grouped_data = int_grouped(int_id, group)
                thread_creat(int(group), int_grouped_data,IntStrInput)
            return
    else:
        print('获取节点列表失败')


def django_test():
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
                    group = round(int_num / CONSTANT.group_interval+1, 0)
                    int_grouped_data = int_grouped(int_id, group)
                    return group, int_grouped_data, IntStrInput
                finally:
                    cr.close()
                    conn.close()
        else:
            print('获取节点列表失败')
    try:
        group, int_list, scats_input = get_scats_int()
    except Exception as e:
        print(e)
    else:
        print('success')
        print(group)
        thread_creat(group,int_list,scats_input)



if __name__ =='__main__':
    # lock = threading.Lock()
    # MainProcess()
    django_test()

lock = threading.Lock()