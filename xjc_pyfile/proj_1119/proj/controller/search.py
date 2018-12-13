# -*- coding: utf-8 -*-
from django.http import HttpResponse
from django.shortcuts import render_to_response
import json
from django.http import JsonResponse
from ..interfaceImpl.TestModelTestSvcImpl import *
from ..vo.TestModelTestVO import *


def severControl(request):
    todo_list = [
        {"id": "1", "content": "吃饭"},
        {"id": "2", "content": "吃饭"},
    ]
    if request.GET:
        if 'q' in request.GET:
            print(todo_list)
            name=request.GET['q']
            todo_list = {"id": "1", "content":name }
            vo=TestModelTestVO(name)
            impl=TestModelTestSvcImpl()
            impl.addOneRecode(vo)
        else:
            todo_list = {"id": "1", "content": "吃饭"}
    response = JsonResponse(todo_list, safe=False)
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
            # vo=TestModelTestVO(name)
            # impl=TestModelTestSvcImpl()
            # impl.addOneRecode(vo)
        else:
            todo_list = {"id": "1", "content": "吃饭"}
    response = JsonResponse(todo_list, safe=False)
    return response
    # resp = {'errorcode': 100, 'detail': 'Get success'}
    # return HttpResponse(json.dumps(resp), content_type="application/json")
