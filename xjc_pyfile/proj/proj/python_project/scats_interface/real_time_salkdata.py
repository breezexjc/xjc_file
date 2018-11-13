import sys
import time
import stomp
import json
from datetime import datetime
import cx_Oracle


class MyListener2(object):
    def on_message(self, headers, message):
        result14 = json.loads(message)
        m14 = result14['header']
        print(result14)
        if m14 == []:
            pass
        else:
            insert_list1 = []
            ActiveLinkPlan = m14['ActiveLinkPlan']
            ActiveSystemPlan = m14['ActiveSystemPlan']
            ActualCycleTime = m14['ActualCycleTime']
            IsSALK = int(m14['IsSALK'])
            NominalCycleTime = m14['NominalCycleTime']
            time_str = m14['RecvTime']
            time_str1 = time_str[:-4]
            time = datetime.strptime(time_str1, '%Y-%m-%d %H:%M:%S')  # 根据字符串本身的格式进行转换
            d = time.strftime('%Y-%m-%d')
            t = time.strftime('%H:%M:%S')
            RecvDate = d
            RecvTime = t
            RequiredCycleTime = m14['RequiredCycleTime']
            SAVDT = m14['SAVDT']
            Saturation = m14['Saturation']
            StrategicCycleTime = m14['StrategicCycleTime']
            SubSystemID = m14['SubSystemID']
            try:
                k14 = result14['splitPlan']
                SplitPlanRecord = ','.join([str(x) for x in k14['SplitPlanRecord']])
            except KeyError:
                SplitPlanRecord = None
            h14 = result14['salkList']
            if h14 == []:
                SiteID = None
            else:
                SiteID = h14[0]['SiteID']
            list1 = [RecvDate, RecvTime, ActiveLinkPlan, ActiveSystemPlan, ActualCycleTime, IsSALK, NominalCycleTime,
                     RequiredCycleTime, SAVDT, Saturation, StrategicCycleTime, SubSystemID, SplitPlanRecord, SiteID]
            insert_list1.append(list1)
            # print(insert_list1)
            db = cx_Oracle.connect('enjoyor/admin@33.83.100.139/orcl')
            cr = db.cursor()
            for i in insert_list1:
                # print(i)
                sql1 = "insert into STR_MON_HEADER(RecvDate,RecvTime,ActiveLinkPlan, ActiveSystemPlan, ActualCycleTime, IsSALK, NominalCycleTime,"               "RequiredCycleTime, SAVDT, Saturation, StrategicCycleTime, SubSystemID, SplitPlanRecord, SiteID) "               "values (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)"
                cr.execute(sql1, i)
                db.commit()

        h14 = result14['salkList']
        if h14 == []:
            pass
        else:
            insert_list2 = []
            for z in h14:
                ADS = z['ADS']
                DS = ','.join([str(x) for x in z['DS']])
                IsSALK = z['IsSALK']
                IsSignalGroup = int(z['IsSignalGroup'])
                PhaseBitMask = z['PhaseBitMask']
                PhaseTime = z['PhaseTime']
                SALKNumber = z['SALKNumber']
                SignalGroupNum = z['SignalGroupNum']
                SiteID = z['SiteID']
                VK = ','.join([str(x) for x in z['VK']])
                VO = ','.join([str(x) for x in z['VO']])
                list2 = [RecvDate, RecvTime, ADS, DS, IsSALK, IsSignalGroup, PhaseBitMask, PhaseTime, SALKNumber,
                         SignalGroupNum, SiteID, VK, VO]
                insert_list2.append(list2)
            # print(insert_list2)
            db = cx_Oracle.connect('enjoyor/admin@33.83.100.139/orcl')
            cr = db.cursor()
            for i in insert_list2:
                # print(i)
                sql2 = "insert into STR_MON_SALKLIST(RecvDate, RecvTime, ADS, DS, IsSALK, IsSignalGroup, PhaseBitMask, PhaseTime, SALKNumber, SignalGroupNum, SiteID, VK, VO) "               "values (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)"
                cr.execute(sql2, i)
                db.commit()

        # db.close()


conn = stomp.Connection10([('33.83.100.138', 61613)])
conn.set_listener('66', MyListener2())
conn.start()
conn.connect(wait=True)
conn.subscribe(destination='/topic/TOPIC_StrategicMonitor', id=1, ack='auto')
while True:
    pass
    time.sleep(1)