#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
author: xjc
version: 20181023
project：报警分类及排序_交通状态
"""
import pandas as pd
import numpy as np
# import matplotlib
# from matplotlib import pyplot as plt
IFTEST = True
#解决中文显示问题
# plt.rcParams['font.sans-serif']=['SimHei']
# plt.rcParams['axes.unicode_minus'] = False
scats_data_sample = pd.DataFrame([],columns=['FSTR_INTERSECTID', 'FINT_SA', 'FINT_DETECTORID', 'FSTR_CYCLE_STARTTIME',
                                             'FSTR_CYCLE_STARTTIME', 'FSTR_PHASE', 'FINT_PHASE_LENGTH', 'FINT_CYCLE_LENGTH',
                                             'FINT_DS', 'FINT_ACTUALVOLUME', 'FSTR_DATE', 'FSTR_WEEKDAY',
                                             'FSTR_CONFIGVERSION', 'IS_REPAIR'])

class ST_PRAM():
    a = 0.2
    b = 0.4
    c = 0.4
    base_volume = 3000

from .database import Postgres, Oracle

class TrafficSituation():
    sql = r"SELECT A.*,B.RDSECTID,B.FUNCTIONID FROM (SELECT * FROM HZ_SCATS_OUTPUT WHERE FSTR_DATE =to_date('2018-06-18','yyyy-MM-dd') " \
          r"AND LENGTH(FINT_DETECTORID)<3 ) A LEFT JOIN SL_SCATS_LOOP_INF B ON A.FSTR_INTERSECTID = B.SYSTEMID AND" \
          r" A.FINT_DETECTORID=B.LP_NUMBER"
    sql_rdsect = r"select * from SL_SCATS_LOOP_INF"
    def __init__(self,salklist=None):
        self.ST={}
        self.salklist = salklist
        # result = self.get_loop_inf()
        # self.loop_inf = result


    def get_salklist(self):
        db = Oracle()
        result = db.call_data(TrafficSituation.sql)
        print(result)
        if IFTEST:
            result['IS_REPAIR']= 1
        avg_weight(result)

    def get_loop_inf(self):
        db = Oracle()
        result = db.call_data(TrafficSituation.sql_rdsect)
        return result


def avg_weight(df):
    grouped = df.groupby(['FSTR_INTERSECTID', 'FSTR_CYCLE_STARTTIME', 'RDSECTID'])
    avg_result = {}
    result = []
    for (k1,k2,k3), group in grouped:
        try:
            cycle_time = sorted(group['FINT_CYCLE_LENGTH'].tolist())[0]
            dsm = np.average(group['FINT_DS'].tolist(), weights=group['FINT_ACTUALVOLUME'].tolist())
        except ZeroDivisionError:
            dsm = 0
            pass
        sd = group['FINT_DS'].std()
        volume = group['FINT_ACTUALVOLUME'].sum()
        result.append([k1, k2, k3, dsm, sd, volume, cycle_time])
    dsm_result = pd.DataFrame(result, columns=['FSTR_INTERSECTID', 'FSTR_CYCLE_STARTTIME', 'RDSECTID', 'DSM', 'STD','VOLUMN','FINT_CYCLE_LENGTH'])

    grouped2 = dsm_result.groupby(['FSTR_INTERSECTID','FSTR_CYCLE_STARTTIME'])
    result_int = []
    for (k1,k2), group in grouped2:
        # group.plot('FSTR_CYCLE_STARTTIME',['DSM','STD','VOLUMN'])
        try:
            dsm_int = np.average(group['DSM'].tolist(), weights=group['VOLUMN'].tolist())
        except ZeroDivisionError:
            dsm_int = 0
            pass
        std_int = group['DSM'].std()
        cycle_time = sorted(group['FINT_CYCLE_LENGTH'].tolist())[0]
        volumn_int = group['VOLUMN'].sum()*3600/cycle_time
        if volumn_int>100:
            volumn_int = 100
        result_int.append([k1,k2,dsm_int,std_int,volumn_int])
    dsm_result = pd.DataFrame(result_int, columns=['FSTR_INTERSECTID', 'FSTR_CYCLE_STARTTIME', 'DSM', 'STD',
                                               'VOLUMNRATE'])
    a, b, c = (ST_PRAM.a,ST_PRAM.b,ST_PRAM.c)
    # STD乘以2归一化
    # DS无需归一化
    # 路口流率除以3000，归一化，大于5000则结果置为1
    dsm_result['ST'] = dsm_result['DSM']*a+dsm_result['STD']*2*b+dsm_result['VOLUMNRATE']*c/ST_PRAM.base_volume*100
    grouped3 = dsm_result.groupby(['FSTR_INTERSECTID']).mean()
    # for k1, group in grouped3:
    #     group.plot('FSTR_CYCLE_STARTTIME', ['DSM', 'STD', 'VOLUMNRATE', 'ST'])
    #     plt.title(k1 + '路口')
    #     plt.show()
    print(grouped3)
    return grouped3
    # for i in range(len(dsm_result)):
    #     print(dsm_result.iloc[i])

if __name__ == '__main__':
    T = TrafficSituation()
    ST = T.get_salklist()
    # print(scats_data_sample)
