import requests
import os
import datetime as dt
import time
import json
import pandas as pd


SEVERURL = "33.83.100.138"

PORT = "8080"

SITE_LIST = ["1","11","12","16","17","93","194","526","195","231","232"]

SITEID = "16"

TIMEINTERVAL = 48000

# STIME = "2018-09-05 00:00:00"
STIME = dt.datetime.strftime(dt.datetime.now()-dt.timedelta(seconds=TIMEINTERVAL),"%Y-%m-%d %H:%M:%S")
# ETIME = "2018-09-05 23:59:59"
ETIME = dt.datetime.strftime(dt.datetime.now(),"%Y-%m-%d %H:%M:%S")

INTPRAM_LIST = [i for i in range(1,5,1)]

NAMESPACE = ["getSiteInfo", "getDetectorCounts", "getStrategicmonitor", "getSplitsPlan", "getCyclePlan",
             "getStrategicInput", "getOperatorIntervention", "getSiteStatus"]

PRAMDEMAND = {"getSiteInfo": ["SiteID"], "getDetectorCounts": ["SiteID", "STime", "ETime"],
              "getStrategicmonitor":["SiteID", "STime", "ETime"], "getSplitsPlan": ["SiteID"],
              "getCyclePlan": ["SiteID"], "getStrategicInput": ["SiteID"], "getOperatorIntervention":
                  ["SiteID", "STime", "ETime"], "getSiteStatus": ["SiteID"]}
TASKNAME = ["4.1路口信息","4.2流量信息","4.3战略运行信息", "4.4路口绿信比", "4.5路口周期", "4.6路口战略输入",
            "4.7人工操作记录", "4.8实时相位数据"]
# requests_task = {"4.1路口信息": {}, "4.2流量信息": {}, "4.3战略运行信息": {}, "4.4路口绿信比": {},
#                      "4.5路口周期": {}, "4.6路口战略输入": {}, "4.7人工操作记录": {}, "4.8实时相位数据": {}}

# requests_task = {"4.1路口信息": {"Name":"getSiteInfo"}, "4.2流量信息": {}, "4.3战略运行信息": {}, "4.4路口绿信比": {},
#                      "4.5路口周期": {}, "4.6路口战略输入": {}, "4.7人工操作记录": {}, "4.8实时相位数据": {}}

class RequestData(object):
    def __init__(self, task_num=None):
        self.url = SEVERURL
        self.port = PORT
        self.request_task = {}
        self.tasknum = task_num
        # self.create_url(namespace, pram)
        if task_num is None:
            self.task = TASKNAME
            self.create_task()
            self.params_change()
            self.request_task_run()
        else:
            self.task = [TASKNAME[task_num]]
            self.create_task()
            self.params_change2()
            self.request_task_run()

    def create_task(self):
        print(self.task)
        if self.tasknum is None:
            for num in range(len(self.task)):
                param = PRAMDEMAND[NAMESPACE[num]]
                param_list = []
                for p in range(len(param)):
                    if param[p] == "SiteID":
                        param_list.append(["SiteID", SITEID])
                    elif param[p] == "STime":
                        param_list.append(["STime", STIME])
                    elif param[p] == "ETime":
                        param_list.append(["ETime", ETIME])
                self.request_task[TASKNAME[num]] = {"Name": NAMESPACE[num], "Param": param_list}
        else:
            param = PRAMDEMAND[NAMESPACE[self.tasknum]]
            param_list = []
            for p in range(len(param)):
                if param[p] == "SiteID":
                    param_list.append(["SiteID", SITEID])
                elif param[p] == "STime":
                    param_list.append(["STime", STIME])
                elif param[p] == "ETime":
                    param_list.append(["ETime", ETIME])
            self.request_task[TASKNAME[self.tasknum]] = {"Name": NAMESPACE[self.tasknum], "Param": param_list}
        # print(self.request_task)

    def create_url(self,namespace, pram):
        str_pram = ""
        for name, p in pram:
            str_pram += name + "=" + str(p) + "&"
        str_pram = str_pram[:-1]
        request_url = "http://" + self.url + ":" + self.port + "/" + namespace + ".html?" + str_pram
        return request_url

    def params_change(self):
        self.requests_url = []
        self.requests_add = []
        print("=========================Address=============================")
        for name in NAMESPACE:
            pname = PRAMDEMAND[name]
            pram = []
            for p in range(len(pname)):
                if pname[p] == "SiteID":
                    pram.append(["SiteID", SITEID])
                elif pname[p] == "STIME":
                    pram.append(["STIME", STIME])
                elif pname[p] == "ETIME":
                    pram.append(["ETIME", ETIME])
                else:
                    pass

            url2, pram_dict = create_url2(name, pram)
            address = self.create_url(name, pram)
            print(address)
            print(url2, pram_dict)
            self.requests_add.append(address)
            self.requests_url.append([url2, pram_dict])

    def params_change2(self):
        self.requests_url = []
        self.requests_add = []
        print("=========================Address=============================")
        task = self.request_task.keys()
        for n in task:
            namespace =self.request_task[n]["Name"]
            param = self.request_task[n]["Param"]
            url2, pram_dict = create_url2(namespace, param)
            address = self.create_url(namespace, param)
            print(address)
            # print(namespace, param)
            self.requests_add.append(address)
            self.requests_url.append([url2, pram_dict])

    def request_task_run(self):
        key_list = list(self.request_task.keys())
        print("=========================TaskResult=============================")
        for task in range(len(self.requests_url)):
            add = self.requests_add[task]
            url = self.requests_url[task][0]
            pram = self.requests_url[task][1]
            key = key_list[task]
            self.request_task[key]["url"] = add
            self.request_task[key]["result"] = self.url_request(add, url, pram)
            dtresult = pd.DataFrame(self.request_task[key]["result"]["resultList"])
            print(str(dtresult))
            time.sleep(1)
            json_data = json.dumps(self.request_task[key]["result"], ensure_ascii=False)
            self.write_into_txt(str(dtresult))
        # print(self.task)

    def url_request(self, add, url, pram):
        result = None
        try:
            request_task = requests.get(url=url, params=pram, timeout=4)
            result = request_task.text
            url_content = request_task.url
        except requests.exceptions.ConnectTimeout:
            print(add, "请求超时!")
        else:
            print(url, "请求成功!")
        return json.loads(result)

    def create_url2(self, namespace, pram):
        str_pram = {}
        for name, p in pram:
            str_pram[name] = p
            # str_pram += name+"="+str(p)+"&"
        request_url = "http://" + SEVERURL + ":" + PORT + "/" + namespace+".html"
        return request_url, str_pram

    def __repr__(self):
        json_data = json.dumps(self.request_task, ensure_ascii=False)
        # print(self.task)
        return str(json_data)


    def write_into_txt(self,content):
        with open("E:\jiekouceshi\\"+"4." + str(self.tasknum+1)+".txt","a") as f:
            f.write(content)
            f.write("\n")

# def create_url(namespace, pram):
#     str_pram = ""
#     for name, p in pram:
#         str_pram += name+"="+str(p)+"&"
#     str_pram = str_pram[:-1]
#     request_url = "http://"+SEVERURL+":"+PORT+"/"+namespace+"/?"+str_pram
#     return request_url


def create_url2(namespace, pram):
    str_pram = {}
    for name, p in pram:
        str_pram[name] = p
        # str_pram += name+"="+str(p)+"&"
    request_url = "http://"+SEVERURL+":"+PORT+"/"+namespace+".html"
    return request_url, str_pram


# def url_request(add, url,pram):
#     result = None
#     try:
#         request_task = requests.get(url=url, params=pram,timeout=4)
#         result =request_task.text
#         url_content = request_task.url
#     except requests.exceptions.ConnectTimeout :
#         print(add, "请求超时!")
#     else:
#         print(url, "请求成功!")
#     return result

#
# def create_task():
#     request_task = {}
#     for num in range(len(TASKNAME)):
#         request_task[TASKNAME[num]] = {"Name": NAMESPACE[num],"Pram": PRAMDEMAND[NAMESPACE[num]]}
#     return request_task


if __name__ == "__main__":
    # 指定任务序号 , 默认为None,请求所有接口
    for i in SITE_LIST:
        SITEID = i
        time.sleep(1)
        task_num = 6
        # 创建任务对象
        R = RequestData(task_num)
        # 输出结果
        # print(R)

