import sys
import time
import stomp
import json
from datetime import datetime
import cx_Oracle

# 监听爬取读入数据库
class MyListener1(object):
    def on_message(self, headers, message):
        db = cx_Oracle.connect('enjoyor/admin@33.83.100.139/orcl')
        cr = db.cursor()
        result17=json.loads(message)
        print("message:", message)
        for m17 in result17:
            m18 = list(m17.keys())
            if m18 == ['NominalCycleLength','PushTime','RequiredCycle','SiteId','SubsystemNumber']:
                pass
            elif m18 == ['PushTime','SiteId']:
                pass
            elif 'LampStatus' in m18:
                insert_list13=[]
                CurrentPhase = m17['CurrentPhase']
                LampStatus = m17['LampStatus']
                PhaseChangeOccurrence = int(m17['PhaseChangeOccurrence'])
                PhaseInterval = m17['PhaseInterval']
                PushTime = m17['PushTime']
                SiteID = m17['SiteID']
                StretchPhase = int(m17['StretchPhase'])
                list13 = [CurrentPhase,LampStatus,PhaseChangeOccurrence,PhaseInterval,PushTime,SiteID,StretchPhase]
                insert_list13.append(list13)
                for i in insert_list13:
                    sql2 = "insert into ST_LAMPSTATUS(CurrentPhase,LampStatus,PhaseChangeOccurrence,PhaseInterval,PushTime,SiteID,StretchPhase) "               "values (:0,:1,:2,:3,:4,:5,:6)"
                    try:
                        cr.execute(sql2, i)
                    except Exception as e:
                        print("无效数据，跳过")
                    db.commit()
            else:
                def StrM(i):
                    if i in m18:
                        return m17[i]
                    else:
                        return None
                insert_list11 = []
                CurrentPhase = StrM('CurrentPhase')
                CurrentPhaseInterval = StrM('CurrentPhaseInterval')
                ElapsedPhaseTime = StrM('ElapsedPhaseTime')
                NextPhase = StrM('NextPhase')
                NominalCycleLength = StrM('NominalCycleLength')
                OffsetPlanLocked1 = StrM('OffsetPlanLocked')
                if OffsetPlanLocked1 == None:
                    OffsetPlanLocked = None
                else:
                    OffsetPlanLocked = int(StrM('OffsetPlanLocked'))
                OffsetPlanNumbers = StrM('OffsetPlanNumbers')
                PushTime = StrM('PushTime')
                RemainingPhaseTime = StrM('RemainingPhaseTime')
                RequiredCycle = StrM('RequiredCycle')
                SiteId = StrM('SiteId')
                SplitPlanLocked1 = StrM('SplitPlanLocked')
                if SplitPlanLocked1 == None:
                    SplitPlanLocked = None
                else:
                    SplitPlanLocked = int(StrM('SplitPlanLocked'))
                SplitPlanNumbers = StrM('SplitPlanNumbers')
                SubsystemNumber = StrM('SubsystemNumber')
                list11 = [CurrentPhase,CurrentPhaseInterval,ElapsedPhaseTime,NextPhase,NominalCycleLength,OffsetPlanLocked,OffsetPlanNumbers,
                          PushTime,RemainingPhaseTime,RequiredCycle,SiteId,SplitPlanLocked,SplitPlanNumbers,SubsystemNumber]
                insert_list11.append(list11)
                for i in insert_list11:
                    # print(i)
                    sql1 = "insert into ST_REALTIME_PHASE(CurrentPhase,CurrentPhaseInterval,ElapsedPhaseTime,NextPhase,NominalCycleLength,OffsetPlanLocked,OffsetPlanNumbers,"               "PushTime,RemainingPhaseTime,RequiredCycle,SiteId,SplitPlanLocked,SplitPlanNumbers,SubsystemNumber) "               "values (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)"
                    try:
                        cr.execute(sql1, i)
                    except Exception as e:
                        pass
                        # print('无效数据,跳过')
                    db.commit()


conn = stomp.Connection10([('33.83.100.138', 61613)])
conn.set_listener('70', MyListener1())
conn.start()
conn.connect(wait=True)
conn.subscribe(destination='/topic/TOPIC_SiteStatus', id=1, ack='auto')
while True:
    pass
    time.sleep(2)