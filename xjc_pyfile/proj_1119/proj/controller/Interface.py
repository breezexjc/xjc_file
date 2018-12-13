# -*- coding: utf-8 -*-
from django.http import HttpResponse
from django.shortcuts import render_to_response
# import json
from django.http import JsonResponse
from ..interfaceImpl.TestModelTestSvcImpl import *
from ..vo.TestModelTestVO import *
from ..config.task_registed.task_registed import RegistedTask
# from ..controller.TaskModel import TaskRegister
from ..python_project.ali_alarm.alarm_priority_algorithm3.alarm_auto_dispose import OperateAutoDis
import os, time
import datetime as dt
# import multiprocessing
# from multiprocessing import Queue, Process
from ..controller import TaskModel
import json
from ..config.database import Postgres,Oracle
from ..config.sql_text import SqlText
from ..tools.inter_face_return_manage import inter_face_manage
global task_inf
import re
import logging
# from ..python_project.ali_alarm import main as ali_main
# log = logging.getLogger('scats')

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
            name=request.GET['q']
            todo_list = {"id": "1", "content": name}
            vo=TestModelTestVO(name)
            impl=TestModelTestSvcImpl()
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

    if request.GET:
        json_demo2 = {'appcode': True, 'result': []}
        sql = SqlText.sql_getscats_operate

        if 'scatsId' in request.GET:
            inter_id = request.GET['scatsId']
            if inter_id:
                localtime = dt.datetime.now()
                localtime = dt.datetime.strptime('2018-10-30 15:00:00', '%Y-%m-%d %H:%M:%S')
                today = localtime.date()
                stime = str(today) + ' 00:00:00'
                etime = dt.datetime.strftime(localtime, '%Y-%m-%d %H:%M:%S')
                sql = sql.format(str(inter_id), stime, etime)
                pg = Postgres.get_instance()
                result = pg.call_pg_data(sql)
                print(result)
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
        response = JsonResponse(json_demo2, safe=False, json_dumps_params={'ensure_ascii': False})
        return response
    else:
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response

def getAlarmAuto(request):
    global autoAlarm
    json_demo = {'appcode': False, 'result': []}
    if request.GET:
        json_demo2 = {'appcode': True, 'result': []}
        if 'intList' in request.GET:
            inter_id = request.GET['intList']
            print(inter_id)
            result = json.loads(inter_id)
            print(result)
            int_list = []
            for i in result:
                int_id = i['interId']
                int_list.append(int_id)
            try:
                result = autoAlarm.alarm_auto_judge(int_list)
            except Exception as e:
                print(e)
            print(autoAlarm.int_auto)
            json_demo2['result'] = result
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
        if 'scatsID' in request.GET:
            result = {'appCode': 2, 'Result': 0}
            scatsid = request.GET.get('scatsID')
            print('getAvailability---scatsid', scatsid)
            if scatsid == 'all':
                sql = "SELECT FSTR_INTERSECTID,to_char(SUM(PER_SCORE), '99.99') SCORE FROM(SELECT A.*,ROUND(10/B.LANENUM,2)*WEIGHT PER_SCORE FROM(" \
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
                sql = "select a.FSTR_INTERSECTID, B.SITENAME, to_char(A.SCORE*10) SCORE from AVILABLE_SCORE a LEFT JOIN " \
                      "INTERSECT_INFORMATION B ON a.FSTR_INTERSECTID = B.SITEID " \
                      "WHERE a.FSTR_DATE = '{0}' ORDER BY TO_NUMBER(A.SCORE) DESC"
                date = str(dt.datetime.now() - dt.timedelta(days=1))[:10]
                # print('date', date)
                before_nowtime = "2018-11-12"
                # if 'ScatsID' in request.GET:
                # sql = sql.format(before_nowtime, str(scatsid))
                sql = sql.format(before_nowtime)
                # print(sql)
                # score = 80
                try:
                    O1 = Oracle()
                    # print('here')
                    # print(sql)
                    result_df = O1.call_oracle_data(sql, fram=False)
                    # print('result_df', result_df)
                    result_json = result_df.to_json(orient='index', force_ascii=False)
                    # print('result_json', result_json)
                    # if result_df:
                    #     print(float(result_df[0][1]))
                    #     score = round(float(result_df['SCORE'][0]), 2) * 10
                    # else:
                    #     score = 'None'
                    # print(score)
                    O1.db_close()
                except Exception as e:
                    print('e ', e)

                # print('result_df', result_df)
                # id = str(result['FSTR_INTERSECTID'][0])
                # print()

            # result = {'appCode': 1, 'Result': {'ScatsID': scatsid, 'AvilableRate': score}}
            result = result_json
    response = JsonResponse(result, safe=False)
    # response = result_json
    return response


def demo(request):
    json_demo = {'appcode': True, 'result': ['澳门首家线上赌场倒闭啦']}
    response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
    return response


def getInterfaceStatus(request):
    # print(request.method)

    if request.method == 'GET':
        if 'sTime' in request.GET and 'eTime' in request.GET:
            # json_demo = {'appcode': True, 'message': '请求成功，参数无误！', 'result': []}
            sTime = request.GET['sTime']
            eTime = request.GET['eTime']
            # print(sTime,type(sTime))
            # print(eTime)
            pg = Postgres(pg_inf=SqlText.pg_inf_arith)
            result = pg.call_pg_data(SqlText.sql_get_interface_status.format(sTime,eTime), fram=True)
            dict = result.to_dict(orient='records')
            json_demo = {'appcode': True, 'message': '请求成功，参数无误！', 'result': dict}
        else:
            json_demo = {'appcode': False, 'message': '请求失败，参数有误！', 'result': []}
            pass
        response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
        return response
    elif request.POST:
        json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确', 'result': []}
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response

@inter_face_manage
def getParseFailed(request):
    json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确!', 'result': []}
    print(request.method)

    if request.method == 'GET':
        if 'date' in request.GET:
            date = request.GET['date']
            pg = Postgres(pg_inf=SqlText.pg_inf_arith)
            print(date)
            try:
                assert re.search(r"(\d{4}-\d{1,2}-\d{1,2})", date), '日期格式错误'
                result = pg.call_pg_data(SqlText.sql_get_parse_failed_detector.format(date), fram=True)
            except AssertionError:
                print("参数格式有误")
                json_demo = {'appcode': False, 'message': '请求失败，参数有误！', 'result': []}
            else:
                dict = result.to_dict(orient='records')
                json_demo = {'appcode': True, 'message': '请求成功，参数无误！', 'result': dict}
            response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
            return response
        else:
            json_demo = {'appcode': False, 'message': '请求失败，参数有误！', 'result': []}
            response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
            return response

    elif request.POST:

        json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确！', 'result': []}
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response

@inter_face_manage
def getScheTask(request):
    json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确!', 'result': []}
    print(request.method)
    if request.method == 'GET':

        pg = Postgres(pg_inf=SqlText.pg_inf_django)
        result = pg.call_pg_data(SqlText.sql_sche_check, fram=True)
        dict = result.to_dict(orient='records')
        json_demo = {'appcode': True, 'message': '请求成功，参数无误！', 'result': dict}
        response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
        return response

    elif request.POST:

        json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确！', 'result': []}
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response

@inter_face_manage
def getLaneStatus(request):
    json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确!', 'result': []}
    print(request.method)
    if request.method == 'GET':
        ora = Oracle()
        result = ora.call_oracle_data(SqlText.sql_get_lane_status, fram=True)
        result.columns = ['scats_id', 'detector_id','date','time','cycle','BLC','MLC','OLC','VOLUMN']
        dict = result.to_dict(orient='records')
        json_demo = {'appcode': True, 'message': '请求成功，参数无误！', 'result': dict}

        response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
        return response

    elif request.POST:

        json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确！', 'result': []}
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response



def getRequestManage(request):
    json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确!', 'result': []}
    print(request.method)
    if request.method == 'GET':
        pg = Postgres(SqlText.pg_inf_django)
        result = pg.call_pg_data(SqlText.sql_get_request_manage, fram=True)
        # result.columns = ['scats_id', 'detector_id','date','time','cycle','BLC','MLC','OLC','VOLUMN']
        dict = result.to_dict(orient='records')
        json_demo = {'appcode': True, 'message': '请求成功，参数无误！', 'result': dict}
        response = JsonResponse(json_demo, safe=True, json_dumps_params={'ensure_ascii': False})
        return response

    elif request.POST:

        json_demo = {'appcode': False, 'message': '请求失败，请检查请求方式是否正确！', 'result': []}
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response
