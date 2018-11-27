# -*- coding: utf-8 -*-
from django.http import HttpResponse
from django.shortcuts import render_to_response
import json
from django.http import JsonResponse
from ..interfaceImpl.TestModelTestSvcImpl import *
from ..vo.TestModelTestVO import *
from ..config.task_registed.task_registed import RegistedTask
from ..controller.TaskModel import TaskRegister
import os, time
import datetime as dt
import multiprocessing
from multiprocessing import Queue, Process
from ..controller import TaskModel
import json
from ..config.database import Postgres, Oracle
from ..config.sql_text import SqlText
import logging
global task_inf
from ..python_project.ali_alarm.alarm_priority_algorithm1126.alarm_auto_dispose import OperateAutoDis
from ..python_project.ali_alarm import main as ali_main
from multiprocessing import Process
log = logging.getLogger('scripts')


task_inf = {}
RUNNING_TASK = {}
OperateStatus = False
autoAlarm = OperateAutoDis()

def getJson2(request):
    global task_inf
    todo_list = [
        {"id": "1", "content": "吃饭"},
        {"id": "2", "content": "吃饭"},
    ]
    if request.GET:
        if 'Interface' in request.GET and 'Flag' in request.GET:
            interface_name = request.GET['Interface']
            control_flag = request.GET['Flag']
            print(interface_name, control_flag)
            if interface_name in RegistedTask['TaskName']:
                if control_flag == "start" and interface_name not in RUNNING_TASK.keys():
                    task = TaskModel.TaskRegister(interface_name)
                    task.start_task()
                    RUNNING_TASK[interface_name] = task
                    task.check_status()
                    task_inf[interface_name] = task.task_state
                elif control_flag == "stop" and interface_name in RUNNING_TASK.keys():
                    task = RUNNING_TASK[interface_name]
                    task.stop_task()
                    RUNNING_TASK.pop(interface_name)

                elif control_flag == "restart" and interface_name in RUNNING_TASK.keys():
                    task = RUNNING_TASK[interface_name]
                    task.restart_task()
                elif control_flag == "state" and interface_name not in RUNNING_TASK.keys():
                    task = TaskModel.TaskRegister(interface_name)
                    task_inf[interface_name] = task.task_state

            todo_list = {"id": "1", "content": interface_name}
            # vo = TestModelTestVO(interface_name, "Running")
            # impl = TestModelTestSvcImpl()
            # impl.addOneRecode(vo)
        else:
            todo_list = {"id": "1", "content": "吃饭"}

    # print(task_inf)
    response = JsonResponse(task_inf, safe=False)
    return response
    # resp = {'errorcode': 100, 'detail': 'Get success'}
    # return HttpResponse(json.dumps(resp), content_type="application/json")


def getJson1(request):
    todo_list = [
        {"id": "1", "content": "吃饭"},
        {"id": "2", "content": "吃饭"},
    ]
    if request.GET:
        if 'q' in request.GET:
            print(todo_list)
            name = request.GET['q']
            todo_list = {"id": "1", "content": name}
            vo = TestModelTestVO(name)
            impl = TestModelTestSvcImpl()
            impl.addOneRecode(vo)
        else:
            todo_list = {"id": "1", "content": "吃饭"}
    response = JsonResponse(todo_list, safe=False)
    return response
    # resp = {'errorcode': 100, 'detail': 'Get success'}
    # return HttpResponse(json.dumps(resp), content_type="application/json")


def getOperate(request):
    # json_demo = {'userid':userid,'oper_time':oper_time,'scats_id':scats_id,'oper':oper,'meaning':meaning,
    #              'oper_type':oper_type,'siteid':siteid,'inter_id':inter_id,'inter_name':inter_name,'alarm_time':alarm_time,
    #              'alarm_id':alarm_id,'disp_id':disp_id}
    json_demo = {'appcode': False, 'result': []}
    print("get request!")
    if request.GET:
        json_demo2 = {'appcode': True, 'result': []}
        sql = SqlText.sql_getscats_operate
        if 'scatsId' in request.GET:
            inter_id = request.GET['scatsId']
            local_time = dt.datetime.now()
            edate = dt.datetime.strftime(local_time, '%Y-%m-%d %H:%M:%S')
            sdate = str(local_time.date()) + ' 00:00:00'
            # today = '2018-10-22'
            sql = sql.format(str(inter_id), sdate, edate)
            pg = Postgres()
            result = pg.call_pg_data(sql)
            pg.db_close()
            for i in result:
                operate = {'userId': None, 'userName': None, 'operTime': None, 'oper': None, 'operType': None}
                userid = i[0]
                user_name = i[4]
                oper_desc = i[5]
                operate['userId'] = userid
                if user_name is None:
                    operate['userName'] = '其他单位'
                else:
                    operate['userName'] = user_name
                if oper_desc is None:
                    operate['operType'] = i[3]
                else:
                    operate['operType'] = oper_desc
                oper_time = i[1]
                operate['operTime'] = oper_time.time()
                operate['oper'] = i[2]
                json_demo2['result'].append(operate)

        else:
            json_demo2['appcode'] = False
            # json_result = json.dumps(json_demo2, ensure_ascii=False)
        print("接口返回：", json_demo2)
        response = JsonResponse(json_demo2, safe=False, json_dumps_params={'ensure_ascii': False})
        return response
    else:
        print("接口返回：", json_demo)
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response


def getOperateRatio(request):
    # demo_pg_inf = {'database': "zkr", 'user': "postgres", 'password': "postgres",
    #                'host': "192.168.20.46", 'port': "5432"}
    json_demo = {'appcode': False, 'spilt': [], 'cycle': []}
    # print(request.method)
    if request.method == 'GET':
        json_demo = {'appcode': True, 'spilt': [], 'cycle': []}
        sql1 = SqlText.sql_get_spilt_number
        sql2 = SqlText.sql_get_cycle_number
        pg = Postgres.get_instance()
        result1 = pg.call_pg_data(sql1)
        result2 = pg.call_pg_data(sql2)

        # print(result1)
        # print(result2)
        x1 = 0
        y1 = 0
        z1 = 0
        operate = [{'text': '自适应方案', 'value': ''}, {'text': '固定方案', 'value': ''}, {'text': '其他方案', 'value': ''}]
        if result1 is not None:
            for i in result1:
                if i[0] == 'adaptive':
                    x1 = i[1]
                if i[0] == 'fixed':
                    y1 = i[1]
                if i[0] == 'other':
                    z1 = i[1]
            if x1 == 0 and y1 == 0 and z1 == 0:
                x1 = 1
                y1 = 1
                z1 = 1
            adaptive = round((x1 / (x1 + y1 + z1)) * 100,1)
            fixed = round((y1 / (x1 + y1 + z1)) * 100,1)
            other = round((z1 / (x1 + y1 + z1)) * 100,1)
            operate[0]['value'] = adaptive
            operate[1]['value'] = fixed
            operate[2]['value'] = other
            json_demo['spilt'] = operate
            x2 = 0
            y2 = 0
        if result2 is not None:
            for i in result2:
                operate = [{'text': '自适应方案', 'value': ''}, {'text': '固定方案', 'value': ''}]
                if i[0] == 'unlock':
                    x2 = i[1]
                if i[0] == 'lock':
                    y2 = i[1]
            if x2 == 0 and y2 == 0:
                x2 = 1
                y2 = 1
            adaptive = round((x2 / (x2 + y2)) * 100,1)
            fixed = round((y2 / (x2 + y2)) * 100,1)
            operate[0]['value'] = adaptive
            operate[1]['value'] = fixed
            json_demo['cycle'] = operate

        response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
        return response
    elif request.POST:
        # print('无法POST')
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response
        # response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
        # return response
    
    
    
def getAlarmAuto(request):
    global autoAlarm
    json_demo = {'appcode': False, 'result': []}
    if request.GET:
        json_demo2 = {'appcode': True, 'result': []}
        if 'intList' in request.GET:
            inter_id = request.GET['intList']
            log.info(inter_id)
            # print('inter_id',inter_id)
            try:
                result = json.loads(inter_id)
            except Exception as e:
                message = "json数据格式有误"
                json_demo2['result'].append(message)
            else:
                try:
                    # print("result", result)
                    int_list = []
                    for i in result:
                        int_id = i['interId']
                        int_list.append(int_id)
                    result = autoAlarm.alarm_auto_judge(int_list)
                    print('result', result)
                except Exception as e:
                    print(e)
                    message = "计算异常，超时"
                    json_demo2['result'].append(message)
                   
                else:
                    # print(autoAlarm.int_auto)
                    # print(autoAlarm.int_auto)
                    json_demo2['result'] = result
            finally:
                log.info(json_demo2)
                response = JsonResponse(json_demo2, safe=False, json_dumps_params={'ensure_ascii': False})
                return response
    else:
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response


#获取可用率接口
def getAvailability(request):
    result = {'appCode': 0, 'Result': 0}
    if request.GET:
        result = {'appCode': 1, 'Result': 0}
        if 'ScatsID' in request.GET:
            sql = "SELECT FSTR_INTERSECTID,SUM(PER_SCORE) SCORE FROM(SELECT A.*,ROUND(10/B.LANENUM,2)*WEIGHT PER_SCORE FROM(" \
                  "SELECT FSTR_DATE, FSTR_INTERSECTID,FINT_DETECTORID,FSTR_ERRORNAME,FSTR_ERRORDETAIL," \
                  "CASE WHEN FSTR_ERRORCODE='NORMSTA' OR FSTR_ERRORCODE='WRN0010'THEN 1 " \
                  "WHEN FSTR_ERRORCODE='ERR0001' THEN 0 WHEN FSTR_ERRORCODE='ERR0002'OR FSTR_ERRORCODE='ERR0003' " \
                  "OR FSTR_ERRORCODE='ERR0004'THEN 0 ELSE 0.5 END WEIGHT FROM HZ_SCATS_DETECTOR_STATE " \
                  "WHERE FSTR_DATE= '{0}')A LEFT JOIN(SELECT FSTR_DATE,FSTR_INTERSECTID,COUNT(*)LANENUM " \
                  "FROM HZ_SCATS_DETECTOR_STATE WHERE FSTR_DATE='{0}' GROUP BY FSTR_DATE,FSTR_INTERSECTID)B " \
                  "ON A.FSTR_DATE = B.FSTR_DATE AND A.FSTR_INTERSECTID = B.FSTR_INTERSECTID " \
                  "ORDER BY A.FSTR_INTERSECTID) WHERE FSTR_INTERSECTID='{1}'GROUP BY FSTR_DATE,FSTR_INTERSECTID"
            # OracleUser = 'SIG_OPT_ADMIN/admin@192.168.20.56/orcl'
            # OracleUser = 'enjoyor/admin@33.83.100.139/orcl'
            # nowtime = dt.datetime.now()
            # before_nowtime = (nowtime+dt.timedelta(days=-1)).strftime("%Y-%m-%d")
            date = str(dt.datetime.now() - dt.timedelta(days=1))[:10]
            print(date)
            before_nowtime = "2018-11-17"
            # if 'ScatsID' in request.GET:
            scatsid = request.GET.get('ScatsID')
            sql = sql.format(before_nowtime, str(scatsid))
            
            O1 = Oracle()
            result = O1.call_oracle_data(sql, fram=True)
            # print(sql)
            # print(result)
            id = str(result['FSTR_INTERSECTID'][0])
            # print()
            score = "%.2f%%" % (result['SCORE'][0]*10)
            O1.db_close()
            result = {'appCode': 1, 'result': {'ScatsID': id, 'AvilableRate': score}}
    response = JsonResponse(result, safe=False)
    return response
