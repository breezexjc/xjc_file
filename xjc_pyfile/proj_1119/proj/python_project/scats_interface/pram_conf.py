sql_get_salklist_status = """
select count(*) from HZ_SCATS_OUTPUT where fstr_date =to_date(: a,'yyyy-MM-dd')  and 
FSTR_CYCLE_STARTTIME between :b and :f
"""
#
sql_get_opetate_status = """
select count(*) from GET_MANOPERATION_RECORD where OPERTIME between :a and :b
"""

sql_failed_detector = """
select distinct FSTR_INTERSECTID,FINT_SA,FINT_DETECTORID,FSTR_DATE  from HZ_SCATS_OUTPUT where fstr_date =to_date(: a,'yyyy-MM-dd')
and FINT_DETECTORID> :b
"""

sql_send_message = """
insert into interface_status_check(interface_name,record_time,data_num,exception) values(%s,%s,%s,%s)
"""

sql_send_parse_failed_detector = """
insert into parse_failed_detector_list(scats_id,salk_no,detector,record_time) values(%s,%s,%s,%s)
"""
# 对缺失数据的时段进行检测
sql_loss_data_period = """
select * from interface_status_check where record_time between '{1}'and '{2}' and interface_name = '{0}'
"""

interface_list = {
    'operate':
    {'url':"http://33.83.100.138:8080/getOperatorIntervention.html",'payload':{'STime': None, 'ETime': None}},
    'salklist':
    {'url':"http://33.83.100.138:8080/getStrategicmonitor.html",'payload':{'SiteID': None, 'STime': None, 'ETime': None}},

}
OracleUser = 'SIG_OPT_ADMIN/admin@192.168.20.56/orcl'

url_interface_operate = "http://33.83.100.138:8080/getOperatorIntervention.html"
payload_operate = {'STime': None, 'ETime': None}

url_salklist = "http://33.83.100.138:8080/getStrategicmonitor.html"
payload_salklist = {'SiteID': None, 'STime': None, 'ETime': None}


parse_failed_judge = 100
IF_TEST = True
TEST_DATE = '2018-11-30'


class CONSTANT:
    REQUEST_INTERVAL = '15M'
    IF_CHECK = False
    IF_DATA_REPAIR = False
    S_REPAIR_DATE = '2018-10-14 20:30:00'
    E_REPAIR_DATE = '2018-10-15 15:00:00'
    request_interval = 300
    group_interval = 200
    TimeDelta = 360
    TimeDelay = 0
    OracleUser = 'enjoyor/admin@33.83.100.139/orcl'
    # OracleUser = 'SIG_OPT_ADMIN/admin@192.168.20.56/orcl'
    pg_inf = {'database': "superpower", 'user': "postgres", 'password': "postgres",
              'host': "172.20.251.98", 'port': "5432"}
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
                     "values (to_date(:0,'yyyy-MM-dd'),:1,:3,:4,:5,:2,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20)"
    sql_RunStrInfoSalkList = "insert into RUN_STR_INFORMATION_SALKLIST1(RecvDate, RecvTime, SITEID_T, ADS, DS1, DS2, " \
                             "DS3, DS4, ID, ISSALK, PHASEBITMASK, PHASETIME, SALKNO, SITEID, VK1, VK2, VK3, VK4, " \
                             "VO1, VO2, VO3, VO4, SMID) values (to_date(:0,'yyyy-MM-dd'),:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14," \
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
