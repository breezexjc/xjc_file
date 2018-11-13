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
from ..config.database import Postgres
from ..config.sql_text import SqlText
global task_inf
from ..python_project.ali_alarm import main as ali_main
from multiprocessing import Process


task_inf = {}
RUNNING_TASK = {}
OperateStatus = False

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
        sql = SqlText.sql_operate_match
        if 'interId' in request.GET:
            inter_id = request.GET['interId']
            if inter_id:
                today = dt.datetime.now().date()
                today = '2018-10-22'
                sql = sql.format(str(inter_id), today)
                pg = Postgres.get_instance()
                result = pg.call_pg_data(sql)
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

            # json_result = json.dumps(json_demo2,ensure_ascii=False)
            # vo = TestModelTestVO(interface_name, "Running")
            # impl = TestModelTestSvcImpl()
            # impl.addOneRecode(vo)
        else:
            json_demo2['appcode'] = False
            # json_result = json.dumps(json_demo2, ensure_ascii=False)
        response = JsonResponse(json_demo2, safe=False, json_dumps_params={'ensure_ascii': False})
        return response
    else:
        response = JsonResponse(json_demo, safe=False, json_dumps_params={'ensure_ascii': False})
        return response







