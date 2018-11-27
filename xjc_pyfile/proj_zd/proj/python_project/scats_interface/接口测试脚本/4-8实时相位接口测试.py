# coding: utf-8
import multiprocessing
import os, time, random
import requests
import json
import datetime as dt
import cx_Oracle
import threading
import pandas as pd
from multiprocessing import Process, Lock, Queue
import psycopg2
import copy
threading.TIMEOUT_MAX = 15
# 线程锁
lock = threading.Lock()
from cProfile import Profile
# 创建计时装饰器
file_name = r"C:\Users\Administrator\Desktop\4.8接口测试结果64.txt"
phase_file_name = r"C:\Users\Administrator\Desktop\4.8接口相位统计信息64.txt"
phase_check_file_name = r"C:\Users\Administrator\Desktop\4.8接口相位数据检查64.txt"


def warper(func):
    def count_time():
        time_start = time.time()
        func()
        time_end = time.time()
        print(str(os.getpid())+'totally cost', time_end - time_start)
        with open(file_name, 'a') as f:
            print("*ALLTIME:", time_end - time_start, file=f)

    return count_time
#################################################################################################
class MyThread(threading.Thread):
    def __init__(self,func,args=()):
        super(MyThread,self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result  # 如果子线程不使用join方法，此处可能会报没有self.result的错误
        except Exception:
            return None


# 参数信息
class Content:
    PHASESTARTTIME = 0   # n hour
    PHASEENDTIME = 24
    INTERVAL = 0.5
    INTNUM = 500
    GROUPINT = 50
    DATASAMPLE = False
    FOREVER = True
    IFTEST = False
    table_phase_order = "recom_compute_reference"
    sql_phase_order = "insert into {0}(scats_id,phase_name,split,order_num,create_time,cycle_length) values (%s,%s,%s,%s,%s,%s)".format(table_phase_order)
    SampleData = """ {"appCode":"0","dataBuffer":"成功","resultList":[{"CurrentPhase":"B","CurrentPhaseInterval":1,
    "Cyclelength":null,"ElapsedPhaseTime":5,"NextPhase":"C","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":26,"RequiredCycle":100,"SiteId":1024,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":128},{"CurrentPhase":"F","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":15,"NextPhase":"A","NominalCycleLength":180,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":17,"RequiredCycle":178,"SiteId":1,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":98},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":12,"NextPhase":"A","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":32,"RequiredCycle":100,"SiteId":1025,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":89},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":27,"NextPhase":"B","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":15,"RequiredCycle":80,"SiteId":1026,"SplitPlanLocked":false,"SplitPlanNumbers":4,"SubsystemNumber":135},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":26,"NextPhase":"A","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":20,"RequiredCycle":100,"SiteId":1027,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":90},{"CurrentPhase":"A","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":30,"NextPhase":"C","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":10,"RequiredCycle":120,"SiteId":1028,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":91},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":35,"NextPhase":"B","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":29,"RequiredCycle":123,"SiteId":1029,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":96},{"CurrentPhase":"B","CurrentPhaseInterval":6,"Cyclelength":null,"ElapsedPhaseTime":66,"NextPhase":"A","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":10238,"RequiredCycle":100,"SiteId":1030,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":97},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":37,"NextPhase":"B","NominalCycleLength":150,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":37,"RequiredCycle":130,"SiteId":1031,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":136},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":67,"NextPhase":"B","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":15,"RequiredCycle":122,"SiteId":1032,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":98},{"CurrentPhase":"A","CurrentPhaseInterval":6,"Cyclelength":null,"ElapsedPhaseTime":53,"NextPhase":"B","NominalCycleLength":108,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":2,"RequiredCycle":112,"SiteId":1033,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":161},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":22,"NextPhase":"B","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":38,"RequiredCycle":100,"SiteId":1034,"SplitPlanLocked":false,"SplitPlanNumbers":4,"SubsystemNumber":132},{"CurrentPhase":"D","CurrentPhaseInterval":1,"Cyclelength":null,"ElapsedPhaseTime":4,"NextPhase":"C","NominalCycleLength":180,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":11,"RequiredCycle":168,"SiteId":10,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":27},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":27,"NextPhase":"B","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":61,"RequiredCycle":121,"SiteId":1035,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":133},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":30,"NextPhase":"B","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":13,"RequiredCycle":140,"SiteId":1036,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":134},{"CurrentPhase":"C","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":23,"NextPhase":"A","NominalCycleLength":110,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":7,"RequiredCycle":91,"SiteId":1037,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":119},{"CurrentPhase":"A","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":55,"NextPhase":"B","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":10,"RequiredCycle":120,"SiteId":1038,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":137},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":6,"NextPhase":"B","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":65,"RequiredCycle":120,"SiteId":1039,"SplitPlanLocked":false,"SplitPlanNumbers":4,"SubsystemNumber":138},{"CurrentPhase":"A","CurrentPhaseInterval":0,"Cyclelength":null,"ElapsedPhaseTime":250,"NextPhase":"B","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":379,"RequiredCycle":80,"SiteId":1040,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":139},{"CurrentPhase":"A","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":57,"NextPhase":"B","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":9,"RequiredCycle":82,"SiteId":1041,"SplitPlanLocked":false,"SplitPlanNumbers":2,"SubsystemNumber":140},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":8,"NextPhase":"B","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":42,"RequiredCycle":100,"SiteId":1042,"SplitPlanLocked":false,"SplitPlanNumbers":4,"SubsystemNumber":99},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":8,"NextPhase":"A","NominalCycleLength":140,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":69,"RequiredCycle":140,"SiteId":1043,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":100},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":16,"NextPhase":"A","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":32,"RequiredCycle":100,"SiteId":1044,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":101},{"CurrentPhase":"A","CurrentPhaseInterval":6,"Cyclelength":null,"ElapsedPhaseTime":20,"NextPhase":"B","NominalCycleLength":40,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":1,"RequiredCycle":40,"SiteId":1045,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":135},{"CurrentPhase":"C","CurrentPhaseInterval":6,"Cyclelength":null,"ElapsedPhaseTime":17,"NextPhase":"A","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":3,"RequiredCycle":80,"SiteId":1046,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":136},{"CurrentPhase":"D","CurrentPhaseInterval":1,"Cyclelength":null,"ElapsedPhaseTime":12,"NextPhase":"E","NominalCycleLength":170,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":24,"RequiredCycle":149,"SiteId":1047,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":137},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":13,"NextPhase":"B","NominalCycleLength":50,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":14,"RequiredCycle":50,"SiteId":1048,"SplitPlanLocked":false,"SplitPlanNumbers":4,"SubsystemNumber":162},{"CurrentPhase":"A","CurrentPhaseInterval":7,"Cyclelength":null,"ElapsedPhaseTime":31,"NextPhase":"B","NominalCycleLength":150,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":1,"RequiredCycle":134,"SiteId":1049,"SplitPlanLocked":false,"SplitPlanNumbers":4,"SubsystemNumber":163},{"CurrentPhase":"A","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":30,"NextPhase":"B","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":12,"RequiredCycle":120,"SiteId":1050,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":138},{"CurrentPhase":"B","CurrentPhaseInterval":1,"Cyclelength":null,"ElapsedPhaseTime":1,"NextPhase":"C","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":20,"RequiredCycle":120,"SiteId":1051,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":139},{"CurrentPhase":"A","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":29,"NextPhase":"B","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":9,"RequiredCycle":120,"SiteId":1052,"SplitPlanLocked":false,"SplitPlanNumbers":2,"SubsystemNumber":142},{"CurrentPhase":"D","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":11,"NextPhase":"A","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":14,"RequiredCycle":120,"SiteId":1053,"SplitPlanLocked":false,"SplitPlanNumbers":2,"SubsystemNumber":143},{"CurrentPhase":"B","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":17,"NextPhase":"C","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":7,"RequiredCycle":100,"SiteId":1054,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":144},{"CurrentPhase":"C","CurrentPhaseInterval":6,"Cyclelength":null,"ElapsedPhaseTime":33,"NextPhase":"A","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":2,"RequiredCycle":120,"SiteId":1055,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":145},{"CurrentPhase":"B","CurrentPhaseInterval":1,"Cyclelength":null,"ElapsedPhaseTime":5,"NextPhase":"C","NominalCycleLength":90,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":24,"RequiredCycle":103,"SiteId":1056,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":146},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":9,"NextPhase":"A","NominalCycleLength":50,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":13,"RequiredCycle":50,"SiteId":1057,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":164},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":43,"NextPhase":"B","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":16,"RequiredCycle":120,"SiteId":1058,"SplitPlanLocked":false,"SplitPlanNumbers":4,"SubsystemNumber":148},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":8,"NextPhase":"C","NominalCycleLength":129,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":18,"RequiredCycle":130,"SiteId":1059,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":149},{"CurrentPhase":"B","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":45,"NextPhase":"A","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":9,"RequiredCycle":120,"SiteId":1060,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":150},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":17,"NextPhase":"A","NominalCycleLength":160,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":46,"RequiredCycle":139,"SiteId":1061,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":107},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":13,"NextPhase":"A","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":33,"RequiredCycle":120,"SiteId":1062,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":165},{"CurrentPhase":"A","CurrentPhaseInterval":6,"Cyclelength":null,"ElapsedPhaseTime":37,"NextPhase":"B","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":5,"RequiredCycle":80,"SiteId":1063,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":151},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":44,"NextPhase":"C","NominalCycleLength":180,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":21,"RequiredCycle":159,"SiteId":1064,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":152},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":45,"NextPhase":"A","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":20,"RequiredCycle":120,"SiteId":1065,"SplitPlanLocked":false,"SplitPlanNumbers":2,"SubsystemNumber":141},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":16,"NextPhase":"B","NominalCycleLength":121,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":43,"RequiredCycle":120,"SiteId":1066,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":142},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":25,"NextPhase":"A","NominalCycleLength":121,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":33,"RequiredCycle":120,"SiteId":1067,"SplitPlanLocked":false,"SplitPlanNumbers":2,"SubsystemNumber":143},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":50,"NextPhase":"A","NominalCycleLength":121,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":23,"RequiredCycle":120,"SiteId":1068,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":144},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":22,"NextPhase":"B","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":41,"RequiredCycle":120,"SiteId":1069,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":145},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":21,"NextPhase":"B","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":48,"RequiredCycle":120,"SiteId":1070,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":146},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":7,"NextPhase":"A","NominalCycleLength":110,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":22,"RequiredCycle":110,"SiteId":1071,"SplitPlanLocked":false,"SplitPlanNumbers":2,"SubsystemNumber":147},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":8,"NextPhase":"B","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":25,"RequiredCycle":80,"SiteId":1072,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":148},{"CurrentPhase":"B","CurrentPhaseInterval":6,"Cyclelength":null,"ElapsedPhaseTime":51,"NextPhase":"A","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":3,"RequiredCycle":120,"SiteId":1073,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":102},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":27,"NextPhase":"A","NominalCycleLength":128,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":31,"RequiredCycle":125,"SiteId":1074,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":103},{"CurrentPhase":"O","CurrentPhaseInterval":0,"Cyclelength":null,"ElapsedPhaseTime":0,"NextPhase":"O","NominalCycleLength":160,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":0,"RequiredCycle":139,"SiteId":1075,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":140},{"CurrentPhase":"C","CurrentPhaseInterval":1,"Cyclelength":null,"ElapsedPhaseTime":1,"NextPhase":"A","NominalCycleLength":90,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":30,"RequiredCycle":80,"SiteId":1076,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":153},{"CurrentPhase":"A","CurrentPhaseInterval":6,"Cyclelength":null,"ElapsedPhaseTime":41,"NextPhase":"B","NominalCycleLength":90,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":1,"RequiredCycle":73,"SiteId":1077,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":154},{"CurrentPhase":"D","CurrentPhaseInterval":6,"Cyclelength":null,"ElapsedPhaseTime":21,"NextPhase":"E","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":4,"RequiredCycle":129,"SiteId":1078,"SplitPlanLocked":false,"SplitPlanNumbers":2,"SubsystemNumber":149},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":33,"NextPhase":"C","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":41,"RequiredCycle":120,"SiteId":1079,"SplitPlanLocked":false,"SplitPlanNumbers":4,"SubsystemNumber":150},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":25,"NextPhase":"C","NominalCycleLength":90,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":20,"RequiredCycle":90,"SiteId":1080,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":151},{"CurrentPhase":"C","CurrentPhaseInterval":1,"Cyclelength":null,"ElapsedPhaseTime":2,"NextPhase":"A","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":17,"RequiredCycle":80,"SiteId":1081,"SplitPlanLocked":false,"SplitPlanNumbers":2,"SubsystemNumber":152},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":7,"NextPhase":"B","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":35,"RequiredCycle":80,"SiteId":1082,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":153},{"CurrentPhase":"B","CurrentPhaseInterval":7,"Cyclelength":null,"ElapsedPhaseTime":24,"NextPhase":"A","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":10239,"RequiredCycle":80,"SiteId":1083,"SplitPlanLocked":false,"SplitPlanNumbers":4,"SubsystemNumber":154},{"CurrentPhase":"D","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":20,"NextPhase":"A","NominalCycleLength":140,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":14,"RequiredCycle":130,"SiteId":1084,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":155},{"CurrentPhase":"B","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":39,"NextPhase":"A","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":8,"RequiredCycle":80,"SiteId":1085,"SplitPlanLocked":false,"SplitPlanNumbers":2,"SubsystemNumber":120},{"CurrentPhase":"B","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":24,"NextPhase":"A","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":11,"RequiredCycle":80,"SiteId":1086,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":156},{"CurrentPhase":"B","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":26,"NextPhase":"A","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":9,"RequiredCycle":80,"SiteId":1087,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":157},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":10,"NextPhase":"B","NominalCycleLength":90,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":27,"RequiredCycle":91,"SiteId":1088,"SplitPlanLocked":false,"SplitPlanNumbers":4,"SubsystemNumber":158},{"CurrentPhase":"C","CurrentPhaseInterval":6,"Cyclelength":null,"ElapsedPhaseTime":28,"NextPhase":"D","NominalCycleLength":160,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":1,"RequiredCycle":149,"SiteId":100,"SplitPlanLocked":false,"SplitPlanNumbers":2,"SubsystemNumber":16},{"CurrentPhase":"D","CurrentPhaseInterval":0,"Cyclelength":null,"ElapsedPhaseTime":17,"NextPhase":"F","NominalCycleLength":160,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":33,"RequiredCycle":149,"SiteId":101,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":4},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":43,"NextPhase":"B","NominalCycleLength":170,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":23,"RequiredCycle":158,"SiteId":102,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":7},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":24,"NextPhase":"C","NominalCycleLength":170,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":27,"RequiredCycle":150,"SiteId":103,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":66},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":23,"NextPhase":"B","NominalCycleLength":180,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":22,"RequiredCycle":159,"SiteId":104,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":8},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":40,"NextPhase":"C","NominalCycleLength":170,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":39,"RequiredCycle":153,"SiteId":105,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":2},{"CurrentPhase":"E","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":33,"NextPhase":"G","NominalCycleLength":180,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":16,"RequiredCycle":163,"SiteId":106,"SplitPlanLocked":false,"SplitPlanNumbers":4,"SubsystemNumber":18},{"CurrentPhase":"A","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":59,"NextPhase":"B","NominalCycleLength":180,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":9,"RequiredCycle":159,"SiteId":107,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":48},{"CurrentPhase":"E","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":33,"NextPhase":"F","NominalCycleLength":160,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":13,"RequiredCycle":177,"SiteId":108,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":49},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":12,"NextPhase":"B","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":59,"RequiredCycle":100,"SiteId":1000,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":85},{"CurrentPhase":"A","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":24,"NextPhase":"B","NominalCycleLength":60,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":10,"RequiredCycle":60,"SiteId":1001,"SplitPlanLocked":false,"SplitPlanNumbers":2,"SubsystemNumber":86},{"CurrentPhase":"A","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":24,"NextPhase":"B","NominalCycleLength":60,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":7,"RequiredCycle":60,"SiteId":1002,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":87},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":6,"NextPhase":"C","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":7,"RequiredCycle":80,"SiteId":1003,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":88},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":18,"NextPhase":"C","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":25,"RequiredCycle":101,"SiteId":1004,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":153},{"CurrentPhase":"B","CurrentPhaseInterval":1,"Cyclelength":null,"ElapsedPhaseTime":2,"NextPhase":"C","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":22,"RequiredCycle":80,"SiteId":1005,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":131},{"CurrentPhase":"A","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":32,"NextPhase":"B","NominalCycleLength":60,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":9,"RequiredCycle":60,"SiteId":1006,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":132},{"CurrentPhase":"A","CurrentPhaseInterval":6,"Cyclelength":null,"ElapsedPhaseTime":36,"NextPhase":"B","NominalCycleLength":60,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":3,"RequiredCycle":60,"SiteId":1007,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":133},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":63,"NextPhase":"B","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":17,"RequiredCycle":101,"SiteId":1008,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":154},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":10,"NextPhase":"C","NominalCycleLength":140,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":21,"RequiredCycle":149,"SiteId":1009,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":155},{"CurrentPhase":"C","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":15,"NextPhase":"A","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":16,"RequiredCycle":120,"SiteId":1010,"SplitPlanLocked":false,"SplitPlanNumbers":4,"SubsystemNumber":134},{"CurrentPhase":"A","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":54,"NextPhase":"B","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":8,"RequiredCycle":92,"SiteId":1011,"SplitPlanLocked":true,"SplitPlanNumbers":0,"SubsystemNumber":58},{"CurrentPhase":"A","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":21,"NextPhase":"B","NominalCycleLength":80,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":10,"RequiredCycle":80,"SiteId":1012,"SplitPlanLocked":false,"SplitPlanNumbers":4,"SubsystemNumber":157},{"CurrentPhase":"B","CurrentPhaseInterval":6,"Cyclelength":null,"ElapsedPhaseTime":22,"NextPhase":"C","NominalCycleLength":144,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":8,"RequiredCycle":144,"SiteId":1013,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":156},{"CurrentPhase":"C","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":11,"NextPhase":"A","NominalCycleLength":107,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":10,"RequiredCycle":107,"SiteId":1014,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":157},{"CurrentPhase":"A","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":19,"NextPhase":"B","NominalCycleLength":107,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":18,"RequiredCycle":100,"SiteId":1015,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":158},{"CurrentPhase":"C","CurrentPhaseInterval":7,"Cyclelength":null,"ElapsedPhaseTime":23,"NextPhase":"A","NominalCycleLength":90,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":4,"RequiredCycle":79,"SiteId":1016,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":159},{"CurrentPhase":"C","CurrentPhaseInterval":1,"Cyclelength":null,"ElapsedPhaseTime":2,"NextPhase":"A","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":30,"RequiredCycle":87,"SiteId":1017,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":123},{"CurrentPhase":"O","CurrentPhaseInterval":0,"Cyclelength":null,"ElapsedPhaseTime":0,"NextPhase":"O","NominalCycleLength":90,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":0,"RequiredCycle":90,"SiteId":1018,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":124},{"CurrentPhase":"O","CurrentPhaseInterval":0,"Cyclelength":null,"ElapsedPhaseTime":0,"NextPhase":"O","NominalCycleLength":90,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":0,"RequiredCycle":90,"SiteId":1019,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":125},{"CurrentPhase":"C","CurrentPhaseInterval":1,"Cyclelength":null,"ElapsedPhaseTime":1,"NextPhase":"A","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":33,"RequiredCycle":100,"SiteId":1020,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":64},{"CurrentPhase":"A","CurrentPhaseInterval":4,"Cyclelength":null,"ElapsedPhaseTime":28,"NextPhase":"B","NominalCycleLength":75,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":8,"RequiredCycle":75,"SiteId":1021,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":160},{"CurrentPhase":"B","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":12,"NextPhase":"C","NominalCycleLength":120,"OffsetPlanLocked":false,"OffsetPlanNumbers":3,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":47,"RequiredCycle":120,"SiteId":1022,"SplitPlanLocked":false,"SplitPlanNumbers":3,"SubsystemNumber":126},{"CurrentPhase":"C","CurrentPhaseInterval":3,"Cyclelength":null,"ElapsedPhaseTime":20,"NextPhase":"A","NominalCycleLength":100,"OffsetPlanLocked":false,"OffsetPlanNumbers":1,"PushTime":"2018-09-10 18:09:57.188","RemainingPhaseTime":22,"RequiredCycle":100,"SiteId":1023,"SplitPlanLocked":false,"SplitPlanNumbers":1,"SubsystemNumber":127}]}"""

    # realtime_phase_url = 'http://192.168.25.23:8080/getSiteStatus.html'
    realtime_phase_url = 'http://33.83.100.138:8080/getSiteStatus.html'
    # realtime_phase_table = "ST_REALTIME_PHASE2"
    OracleUser = 'enjoyor/admin@33.83.100.139/orcl'
    # OracleUser = 'SIG_OPT_ADMIN/admin@192.168.20.56/orcl'
    # pg_inf = {'database': "signal_specialist",'user': "postgres",'password': "postgres",
    #           'host': "192.168.20.46",'port': "5432"}
    # pg_inf = {'database': "xjc", 'user': "postgres", 'password': "postgres",
    #           'host': "192.168.20.56", 'port': "5432"}
    realtime_phase_table_pg = "st_realtime_phase_inf"
    pg_inf = {'database': "signal_specialist", 'user': "postgres", 'password': "postgres",
              'host': "33.83.100.145", 'port': "5432"}


class Oracle(object):
    ip = "192.168.20.56"
    account = "SIG_OPT_ADMIN"
    passoward = "admin"
    port = 5432
    sid = "orcl"
    def __init__(self):
        self.ip = Oracle.ip
        self.account = Oracle.account
        self.passoward = Oracle.passoward
        self.port = Oracle.port
        self.sid = Oracle.sid
    def db_conn(self):
        self.conn = []
        try:
            # enjoyor/admin@33.83.100.139/orcl
            self.conn = cx_Oracle.connect(self.account+'/'+self.passoward+'@'+self.ip+'/'+self.sid)
        except Exception as e:
            print("["+dt.datetime.strftime(dt.datetime.now(),"%Y-%m-%d %H:%M:%S")+"]    Oracle-db_conn:",e)
            # # logging.error("Oracle-db_conn:", e)
        else:
            return self.conn
    def db_close(self):
        try:
            self.conn.close()
        except Exception as e:
            print("Oracle-db_close:", e)
            # logging.error("Oracle-db_close:", e)


class Postgresql(object):
    conn_inf = Content.pg_inf
    ip = conn_inf['host']
    account = conn_inf['user']
    passoward = conn_inf['password']
    port = conn_inf['port']
    database = conn_inf['database']

    def __init__(self):
        self.ip = Postgresql.ip
        self.account = Postgresql.account
        self.passoward = Postgresql.passoward
        self.port = Postgresql.port
        self.database = Postgresql.database

    def db_conn(self):
        try:
            self.conn = psycopg2.connect(database=self.database, user=self.account, password=self.passoward,
                                         host=self.ip,
                                         port=self.port)
        except Exception as e:
            print(self.ip + " connect failed")
            return self.conn, self.cr
        else:
            print(self.ip + " connect succeed")
            self.cr = self.conn.cursor()
            return self.conn, self.cr

    def db_close(self):
        try:
            self.conn.close()
        except Exception as e:
            print("Postgresql-db_close:", e)
            # logging.error("Postgresql-db_close:", e)


def runRealtimePhase(getrealtimephase, list_RealTimePhase):
    GetRealTimePhase = json.loads(getrealtimephase)
    RealTimePhase = GetRealTimePhase["resultList"]
    # list_RealTimePhase = []
    if any(RealTimePhase):
        for i in RealTimePhase:
            Push_Time = i['PushTime']
            CurrentPhase = i['CurrentPhase']
            CurrentPhaseInterval = i['CurrentPhaseInterval']
            Cyclelength = i['Cyclelength']
            ElapsedPhaseTime = i['ElapsedPhaseTime']
            NextPhase = i['NextPhase']
            NominalCycleLength = i['NominalCycleLength']
            if i['OffsetPlanLocked'] == None:
                OffsetPlanLocked = None
            else:
                OffsetPlanLocked = int(i['OffsetPlanLocked'])  # Oracle没有bol型
            OffsetPlanNumbers = i['OffsetPlanNumbers']
            RemainingPhaseTime = i['RemainingPhaseTime']
            RequiredCycle = i['RequiredCycle']
            SiteId = i['SiteId']
            if i['SplitPlanLocked'] == None:
                SplitPlanLocked = None
            else:
                SplitPlanLocked = int(i['SplitPlanLocked'])
            SplitPlanNumbers = i['SplitPlanNumbers']
            SubsystemNumber = i['SubsystemNumber']
            itemlist = [CurrentPhase, CurrentPhaseInterval, Cyclelength, ElapsedPhaseTime, NextPhase, NominalCycleLength,
                      OffsetPlanLocked, OffsetPlanNumbers,Push_Time, RemainingPhaseTime, RequiredCycle, SiteId, SplitPlanLocked,
                      SplitPlanNumbers, SubsystemNumber]
            list_RealTimePhase.append(itemlist)
    else:
        pass
    return list_RealTimePhase


def send2pg(sql, data=None):
    db1 = Postgresql()
    db1_conn, cr = db1.db_conn()
    if db1_conn:

        for i in data:
            # try:
            cr.execute(sql, i)
            # except Exception as e:
            #     print("数据重复", e)
            #     pass
            db1_conn.commit()
        print("数据插入成功")
        db1.db_close()
    else:
        print("send2oracle:数据库连接失败")
        pass
        # logging.error("send2oracle:数据库连接失败")
    pass


def send2oracle(sql, data=None):
    db1 = Oracle()
    db1_conn = db1.db_conn()
    if db1_conn:
        cr = db1_conn.cursor()
        for i in data:
            try:
                cr.execute(sql, i)
                # print(i)
            except Exception as e:
                print("send2oracle:",e)
                # logging.warning("send2oracle:", e)
            db1_conn.commit()
        print("数据插入成功")
        cr.close()
        db1.db_close()
    else:
        print("send2oracle:数据库连接失败")
        # logging.error("send2oracle:数据库连接失败")


def check_phase_null(list_data):
    """检查currentphase为Null的路口"""
    lose_data_int = []
    lose_data = {}
    for int_data in list_data:
        if int_data[0] is None:
            lose_data_int.append(int_data[11])
    localtime = dt.datetime.strftime(dt.datetime.now(), "%Y-%m-%d %H:%M:%S")
    lose_data['RecordTime'] = localtime
    lose_data['LoseData'] = lose_data_int
    return lose_data


def thread_create(url, int_list, q):
    # print("url:", url)
    result_all = []
    count_request = 0
    while True:
        count_request += 1
        result_all = []
        num = 0
        start_time = dt.datetime.now()
        # 限时运行 phase_start 开始请求时间
        local_time = dt.datetime.now()
        start_day = dt.datetime.strptime(str(local_time.date())+" 00:00:00", '%Y-%m-%d %H:%M:%S')
        phase_start = start_day + dt.timedelta(hours=Content.PHASESTARTTIME)
        phase_end = start_day + dt.timedelta(hours=Content.PHASEENDTIME)
        if local_time <= phase_start or local_time >= phase_end:
            time.sleep(60)
            break
        if Content.IFTEST:
            with open(file_name, 'a') as f:
                print("*CountRequest:", count_request, "\tStartTime:", start_time, file=f)
        for group in int_list:
            pram = []
            num += 1
            for i in group:
                pram.append(str(i))
            payload2 = {'SiteID': pram}
            # print(payload2)
            # time.sleep(3)
            result = request_task1(url, payload2, num)
            # print(result)
            if len(result) > 0 and result[0] != 'false':
                if Content.DATASAMPLE:
                    result_all = result
                    print("result_all", result_all)
                    break
                else:
                    result_all = result_all + result
            else:
                # 若出现请求超时，退出程序
                Content.FOREVER = False
                break
            time.sleep(Content.INTERVAL)
        if Content.IFTEST:
            with open(file_name, 'a') as f:
                end_time = dt.datetime.now()
                cost_time = end_time - start_time
                print("*OneRoundTime:", cost_time.seconds, file=f)
        if len(result_all) > 0:
            print("计算相位")
            q.put(result_all)
            print("result_all:", len(result_all))
            # phase_count(result_all)
            sql_oracle = "insert into {0} values (:0,:1,:2,:3,:4,:5,:6,:7,to_timestamp(:8,'yyyy-mm-dd hh24:mi:ss')" \
                         ",:9,:10,:11,:12,:13,:14) ".format(Content.realtime_phase_table_pg)
            sql_pg = "insert into {0} values(%s,%s,%s,%s,%s,%s,%s,%s,to_timestamp(%s,'yyyy-mm-dd hh24:mi:ss'),%s,%s,%s,%s,%s,%s) " \
                .format(Content.realtime_phase_table_pg)
            # send2oracle(sql_oracle, result_all)
            send2pg(sql_pg, result_all)


        if Content.FOREVER:
            check_result = check_phase_null(result_all)
            if Content.IFTEST:
                with open(phase_check_file_name, 'a') as f:
                    print("RecordTime:", dt.datetime.now())
                    print(check_result, file=f)
        else:
            break


def request_task1(url, payload2, num):
    list_RealTimePhase = []
    try:
        start_time = dt.datetime.now()
        # print("start:", start_time)
        if Content.DATASAMPLE:
            GetRealTimePhase = Content.SampleData
        else:
            result = requests.get(url=url, params=payload2, timeout=5)  # 4.8
            GetRealTimePhase = result.text
        # print("end_time:", end_time)
        # url_pram = result.url
        # print(url_pram)
    except requests.exceptions.ConnectTimeout:
        end_time = dt.datetime.now()
        print(end_time, "timeout")
        # NETWORK_STATUS = False
        list_RealTimePhase = ["false"]
    except Exception as e:
        print("request_task1:异常错误", e)
        list_RealTimePhase = ["false"]
    else:
        cost_time = dt.datetime.now() - start_time
        if Content.IFTEST:
            with open(file_name, 'a') as f:
                print("*INTNUM:", Content.INTNUM, "*GROUPINT:", Content.GROUPINT, Content.INTNUM, "*INTERVAL:", Content.INTERVAL ,file=f)
                # print("*UrlPram:", url_pram, file=f)
                print("*Result:", GetRealTimePhase)
                print("*CostTime:", cost_time, file=f)
        print("get data finished ,group {0} cost_time:".format(num), dt.datetime.now() - start_time)
        # print(str(i)+"路口数据获取完成")
        if Content.DATASAMPLE:
            # print("GetRealTimePhase", len(GetRealTimePhase))
            try:
                list_RealTimePhase = runRealtimePhase(GetRealTimePhase, list_RealTimePhase)
            except Exception as e:
                print("list_RealTimePhase", e)
        else:
            try:
                list_RealTimePhase = runRealtimePhase(GetRealTimePhase, list_RealTimePhase)
            except Exception as e:
                print("list_RealTimePhase", e)
        # int_phase_order[i].append(GetRealTimePhase["resultList"]["CurrentPhase"])
    finally:
        return list_RealTimePhase


def call_oracle():
    rs1 = []
    match_records = []
    try:  # 数据库连接超时即退出程序
        print("连接数据库中……")
        db = cx_Oracle.connect(Content.OracleUser)
        cr = db.cursor()
    except cx_Oracle.DatabaseError:
        print('ERROR:数据库连接超时')
        # sys.exit(0)
    else:
        print("Oracle数据库连接成功")
        try:  # 表名错误或日期错误即退出
            sql1 = " select distinct SITEID from INTERSECT_INFORMATION order by SITEID "
            cr.execute(sql1)
            rs1 = cr.fetchall()
            cr.close()
            db.close()
        except cx_Oracle.DatabaseError:
            print('ERROR:数据表名输入错误或不存在')
            # sys.exit(0)

    return rs1


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


@ warper
def create_process():

    # global int_phase_order
    # int_phase_order = {}

    inter_inf = call_oracle()
    int_list = [i[0] for i in inter_inf]
    int_list = int_list[:Content.INTNUM]
    # print(int_list)
    print("路口总数:", len(int_list))

    print("路口数据获取成功")
    # 对所有路口列表按one_group个路口分组
    one_group = Content.GROUPINT
    grouped = int_grouped(int_list, one_group)
    try:
        # function_list = [request_task1, phase_count]
        print("parent process %s" % (os.getpid()))
        # pool = multiprocessing.Pool(6)
        # p1 = Process(target=request_task1, args=[realtime_phase_url,int_list])
        # p2 = Process(target=phase_count, args=[])
        q = Queue()
        try:
            p1 = Process(target=thread_create, args=[Content.realtime_phase_url, grouped, q,])
            # p2 = Process(target=phase_count, args=[q, ])
            p1.start()
            # p2.start()
            p1.join()
            # if not Content.FOREVER:
            #     p2.join()
            # p2.terminate()
            # pool.apply_async(thread_create, args=[Content.realtime_phase_url,grouped ])
            # pool.apply_async(phase_count, args=[])
            # pool.apply_async(delete_data, args=[])
        except Exception as e:
            print("进程出错", e)
            # logging.error("进程出错")
        else:
            print("任务创建成功")
        # pool.close()
        # # Pool执行函数，apply执行函数,当有一个进程执行完毕后，会添加一个新的进程到pool中
        # print('Waiting for all subprocesses done...')
        # pool.join()  # 调用join之前，一定要先调用close() 函数，否则会出错, close()执行后不会有新的进程加入到pool,join函数等待素有子进程结束
        # print('All subprocesses done.')
    except Exception as e:
        print(e)


def phase_order_send(order_result):
    # create_time = dt.datetime.strftime(dt.datetime.now(), '%Y-%m-%d %H:%M:%S')
    send_data = []
    for data in order_result:
        create_time = data['CycleStartTime']
        site_id = data['SiteId']
        cycle_time = data['CycleTime']
        phase_order = data['PhaseOrder']
        order_num = 0
        for phase in phase_order.keys():
            print("keys", phase_order.keys())
            order_num += 1
            phase_length = phase_order[phase]
            send_data.append([site_id, phase, phase_length, order_num, create_time, cycle_time])
    print("相序数据收集完毕")
    print(send_data)
    send2pg(Content.sql_phase_order, send_data)


def phase_inf_resolve(phase_data):
    global order_result, phase_interval
    local_time = dt.datetime.now()
    s_time = local_time - dt.timedelta(minutes=5)
    s_time2str = dt.datetime.strftime(s_time, "%Y-%m-%d %H:%M:%S")
    # itemlist = [CurrentPhase, CurrentPhaseInterval, Cyclelength, ElapsedPhaseTime, NextPhase, NominalCycleLength,
    #             OffsetPlanLocked, OffsetPlanNumbers, Push_Time, RemainingPhaseTime, RequiredCycle, SiteId,
    #             SplitPlanLocked,
    #             SplitPlanNumbers, SubsystemNumber]
    if len(phase_data) > 0:
        print("phase_data", len(phase_data))
        phase_send = []
        for i in phase_data:
            try:
                keys = order_result.keys()
                push_time = i[8]
                elapsed_phase_time = i[3]
                phase_name = i[0]
                cycle_time = i[5]
                siteid = i[11]
                require_cycle = i[10]
                try:
                    phase_start_time = dt.datetime.strftime(push_time - \
                    dt.timedelta(seconds=elapsed_phase_time), '%Y-%m-%d %H:%M:%S')
                except TypeError:
                    pass
                else:
                    # phase_start_time = dt.datetime.strftime(dt.datetime.strptime(push_time[0:19], '%Y-%m-%d %H:%M:%S') - \
                    # dt.timedelta(seconds=elapsed_phase_time), '%Y-%m-%d %H:%M:%S')
                    if siteid is not None:
                        if siteid not in keys:
                            order_result[siteid] = {}
                            phase_interval[siteid] = {}
                            # print(push_time)
                            order_result[siteid]['CycleStartTime'] = phase_start_time
                            order_result[siteid]['SiteId'] = siteid
                            order_result[siteid]['RequireCycle'] = require_cycle
                            order_result[siteid]['CycleTime'] = cycle_time
                            order_result[siteid]['PhaseOrder'] = {}
                            order_result[siteid]['PhaseOrder'][phase_name] = i[3] + i[9]
                        else:
                            if order_result[siteid]['RequireCycle'] != None:
                                if order_result[siteid]['RequireCycle'] != require_cycle:
                                    print(i)
                                    if len(order_result[siteid]['PhaseOrder'].keys()) > 0:
                                        cycle_data = copy.deepcopy(order_result[siteid])
                                        print("cycle_data", cycle_data)
                                        phase_send.append(cycle_data)
                                        order_result[siteid]['CycleStartTime'] = phase_start_time
                                        # phase_order_send(order_result)
                                        order_result[siteid]['RequireCycle'] = require_cycle
                                        order_result[siteid]['CycleTime'] = cycle_time
                                    order_result[siteid]['PhaseOrder'] = {}
                                    order_result[siteid]['PhaseOrder'][phase_name] = i[3] + i[9]
                                else:
                                    order_result[siteid]['CycleTime'] = cycle_time
                                    order_result[siteid]['PhaseOrder'][phase_name] = i[3] + i[9]
                    else:
                        pass
            except Exception as e:
                print(e)
        print("order_result", order_result)
        print("phase_send", phase_send)
        phase_order_send(phase_send)
    else:
        pass
    # except Exception as e:
    #     print(e)
    if Content.IFTEST:
        with open(phase_file_name, 'a') as f:
            print("RecordTime", dt.datetime.now(), file=f)
            print("*order_result:\n", order_result, file=f)
            print('++++++++++++++++++++++++++++++++++++++++++++++')
            print("*phase_interval\n", phase_interval, file=f)


def phase_count(q):
    global order_result, phase_interval
    order_result = {}
    phase_interval = {}
    print("PhaseCount Pid: ", os.getpid())
    while True:
        phase_data = q.get(True)
        print("PhaseCount Get Value Succeed! ")
        phase_inf_resolve(phase_data)
        if not Content.FOREVER:
            break
        # return order_result, phase_interval


# 每隔n小时清理一次表数据,默认24h
def delete_data(n=24):
    last_date = dt.datetime.now().date()
    while True:
        try:
            local_time = dt.datetime.now()
            date = local_time.date()
            if last_date != date:
                sql_delete = "delete from  {1} where PUSHTIME < {0}".format(local_time-dt.timedelta(hours=n), Content.realtime_phase_table_pg)
                last_date = date
                db1 = Postgresql()
                db1_conn, cr = db1.db_conn()
                if db1_conn:
                    try:
                        cr.execute(sql_delete)
                    except Exception as e:
                        print("send2oracle:", e)
                        # logging.warning("send2oracle:", e)
                    db1_conn.commit()
                    db1.db_close()
            time.sleep(1800)
            # print('sleep!')
        except Exception as e:
            print('Exit The Job!', e)


if __name__ == '__main__':
    create_process()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    last_date = dt.datetime.now().date()





