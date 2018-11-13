import psycopg2
# from .database import *
from .database import *
import os
import re
import pandas as pd
from . import operate_record_resolve
import json
import requests
import sys
import time
import psycopg2
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
# import datetime
import cx_Oracle
import threading
from datetime import timedelta,date,datetime
import csv
# from database import pg_inf, OracleUser


KeyWord = {'IP': '绿信比方案', 'CL': '周期', 'SS': '子系统', 'LP': '协调方案', 'LP0': '0号协调方案', 'MG': '协调关系', 'PL': '绿信比方案',
               'DV': '离婚', 'XSF': None, 'SF': None, 'PD': '主相位', 'NGNS': None, 'NSNG': None,
               'NGNG': None, 'PP1': None, 'HCL': '高周期', 'Plan': None, 'Activated': None}

last=[]

def seperate(data):
    END=[]
    for record in data:
        dict = {}
        list=['']*9
        print(record)
        meaning=operate_record_resolve.resolve_operate_data(record)
        oper_code = record[0]   #链接数据库时用这句
        opercode=record[1]
        opertime=record[2]
        opertype=str(record[3])
        region=str(record[4])
        siteid=record[5]
        userid=str(record[6])
        list[0]=oper_code
        list[1]=meaning
        list[2]=opercode
        list[3]=opertime
        list[4] =''
        list[5] =region
        list[7] =userid
        if oper_code:
            print(oper_code)
            # re.match匹配开头
            m0 = re.match(r'([0-9]+): ([0-9]+)!(.*)', oper_code, re.I)
            m1 = re.match(r'SS=([0-9]+) (.*)', oper_code, re.I)
            m2 = re.match(r'IP(.*)', oper_code, re.I)
            m3 = re.match(r'([0-9]+): I=([0-9]+)(.*)', oper_code, re.I)
            m4 = re.match(r'PL(.*)', oper_code, re.I)
            m5 = re.match(r'XSF(.*)', oper_code, re.I)
            m6 = re.match(r'([0-9]+): SS=([0-9]+)(.*)', oper_code, re.I)
            m7 = re.match(r'KEY=([0-9]+) I=([0-9]+)(.*)',oper_code,re.I)
            m8 = re.match(r'KEY=([0-9]+) (SI|SA)=([0-9]+)(.*)',oper_code,re.I)
            m9 = re.match(r'KEY=([0-9]+) ([a-z]+)([0-9]{1,5})=(.{1,5})! \((.{1,10})\)(.*)',oper_code,re.I)
            m10= re.match(r'Term(.*)',oper_code,re.I)
            m11 = re.match(r'KEY=([0-9]+) SS=([0-9]+)(.*)', oper_code, re.I)
            lll=None
            if m0:
                lll='I='+m0.group(2)
            if m3:
                lll='I='+m3.group(2)
            if m1:
                lll='SS='+m1.group(1)
            if m6:
                lll='SS='+m6.group(2)
            if m7:
                lll='I='+m7.group(2)
            if m11:
                lll='SS='+m11.group(2)

            if siteid==None:
                list[6]=lll
            else:
                #读表格数据
                site = siteid
                #读数据库数据
                #site='I='+siteid
                list[6]=site

            if m0:
                extra=m0.group(1)+':'+m0.group(2)+'!'
                #list[19]=extra
                m0_7=re.finditer(r'([a-z]{2,3})=([0-9]+)\^!',m0.group(3),re.I)
                m0_1=re.finditer(r'([a-z]{2,3})=([0-9]+)(#|)!',m0.group(3),re.I)
                m0_2=re.finditer(r'([a-z]{2,3})(/|#.{0,30}!)',m0.group(3),re.I)
                m0_3=re.finditer(r'Plan(.*)',m0.group(3),re.I)
                m0_4=re.finditer(r'([a-z]{2,3})=([+\-])([0-9]+)#(.{0,30})!',m0.group(3),re.I)
                m0_5=re.search(r'(CL|PL)=([0-9]+)(#;|#!;)(.*)',m0.group(3),re.I)
                m0_6=re.finditer(r'XSF=(\+|-)([0-9]+)(.{0,15})!',m0.group(3),re.I)
                dict['extra']=[extra]
                if m0_7:
                    for match in m0_7:
                        location=int(match.span(1)[0])
                        key=match.group(1)
                        message=match.group(2)+'^!'
                        if key in dict.keys():
                            dict[key].append([message,location])
                        else:
                            dict[key] = [[message,location]]
                if m0_6:
                    for match in m0_6:
                        location = int(match.span(1)[0])
                        key='XSF'
                        message=match.group(1)+match.group(2)+match.group(3)+'!'
                        if key in dict.keys():
                            dict[key].append([message,location])
                        else:
                            dict[key] = [[message,location]]
                if m0_5:
                    location = int(m0_5.span(1)[0])
                    key=m0_5.group(1)
                    message=m0_5.group(2)+m0_5.group(3)+m0_5.group(4)
                    if key in dict.keys():
                        dict[key].append([message,location])
                    else:
                        dict[key] = [[message,location]]
                if m0_1:
                    for match in m0_1:
                        location=int(match.span(1)[0])
                        key=match.group(1).upper()
                        message=match.group(2)+match.group(3)+'!'
                        if key in dict.keys():
                            dict[key].append([message,location])
                        else:
                            dict[key] = [[message,location]]
                        # dict[]
                        # if key == 'IP':
                        #     list[2] = list[2]+' '+message
                        # if key == 'CL':
                        #     list[3] = list[3]+' '+message
                        # if key == 'SS':
                        #     list[4] = list[4]+' '+message
                        # if key == 'LP':
                        #     list[5] = list[5]+' '+message
                        # if key == 'LP0':
                        #     list[6] = list[6]+' '+message
                        # if key == 'MG':
                        #     list[7] = list[7]+' '+message
                        # if key == 'PL':
                        #     list[8] = list[8]+' '+message
                        # if key == 'DV':
                        #     list[9] = list[9]+' '+message
                        # if key == 'XSF':
                        #     list[10] = list[10]+' '+message
                        # if key == 'SF':
                        #     list[11] = list[11]+' '+message
                        # if key == 'PD':
                        #     list[12] = list[12]+' '+message
                if m0_2:
                    for match in m0_2:
                        location=int(match.span(1)[0])
                        key=match.group(1).upper()
                        message=match.group(2)
                        if key in dict.keys():
                            dict[key].append([message,location])
                        else:
                            dict[key] = [[message,location]]
                        # if key == 'IP':
                        #     list[2] = list[2]+' '+message
                        # if key == 'CL':
                        #     list[3] = list[3]+' '+message
                        # if key == 'SS':
                        #     list[4] = list[4]+' '+message
                        # if key == 'LP':
                        #     list[5] = list[5]+' '+message
                        # if key == 'LP0':
                        #     list[6] = list[6]+' '+message
                        # if key == 'MG':
                        #     list[7] = list[7]+' '+message
                        # if key == 'PL':
                        #     list[8] = list[8]+' '+message
                        # if key == 'DV':
                        #     list[9] = list[9]+' '+message
                        # if key == 'XSF':
                        #     list[10] = list[10]+' '+message
                        # if key == 'SF':
                        #     list[11] = list[11]+' '+message
                        # if key == 'PD':
                        #     list[12] = list[12]+' '+message
                if m0_3:
                    for match in m0_3:
                        key='Plan'
                        message=match.group(1)
                        location=int(match.span(1)[0])
                        m0_3_1=re.finditer(r'([0-9]+)!(.*)',match.group(1),re.I)
                        m0_3_1_1=re.finditer(r'([a-z]+)=(.{0,8})!',match.group(1),re.I)
                        for match in m0_3_1:
                            message=match.group(1)+'!'
                            if key in dict.keys():
                                dict[key].append([message,location])
                            else:
                                dict[key] = [[message,location]]
                        for match in m0_3_1_1:
                            message=match.group(1)+'='+match.group(2)+'!'
                            if key in dict.keys():
                                dict[key].append([message,location])
                            else:
                                dict[key] = [[message,location]]
                if m0_4:
                    for match in m0_4:
                        location=int(match.span(1)[0])
                        key=match.group(1).upper()
                        message=match.group(2)+match.group(3)+'#'+match.group(4)+'!'
                        if key in dict.keys():
                            dict[key].append([message,location])
                        else:
                            dict[key] = [[message,location]]
                        # if key == 'IP':
                        #     list[2] = list[2]+' '+message
                        # if key == 'CL':
                        #     list[3] = list[3]+' '+message
                        # if key == 'SS':
                        #     list[4] = list[4]+' '+message
                        # if key == 'LP':
                        #     list[5] = list[5]+' '+message
                        # if key == 'LP0':
                        #     list[6] = list[6]+' '+message
                        # if key == 'MG':
                        #     list[7] = list[7]+' '+message
                        # if key == 'PL':
                        #     list[8] = list[8]+' '+message
                        # if key == 'DV':
                        #     list[9] = list[9]+' '+message
                        # if key == 'XSF':
                        #     list[10] = list[10]+' '+message
                        # if key == 'SF':
                        #     list[11] = list[11]+' '+message
                        # if key == 'PD':
                        #     list[12] = list[12]+' '+message
            if m1:
                SS=m1.group(1)
                #list[4]=SS
                dict['SS']=[SS]
                print(list)
                m1_1 = re.finditer(r'([a-z]+)=([0-9]+)(.{1,11})!(.*)', m1.group(2), re.I)
                m1_2 = re.finditer(r'([a-z]+)(/|.{1,12}!)(.*)', m1.group(2), re.I)
                m1_3 = re.finditer(r'LP0=([0-9]+),(.*)', m1.group(2), re.I)
                dict['extra']=[]
                if m1_1:
                    for match in m1_1:
                        location1 = int(match.span(1)[0])
                        location2=int(match.span(4)[0])
                        key1=match.group(1).upper()
                        value=match.group(2)
                        way=match.group(3)
                        extra=match.group(4)
                        if key1 in dict.keys():
                            dict[key1].append([value+way+'!',location1])
                            #dict['extra'].append(extra)
                        else:
                            dict[key1] = [[value+way+'!',location1]]
                        dict['extra'].append(extra)
                        # if key1=='LP':
                        #     list[5]=value+way+'!'
                        #     list[19]=extra
                        # if key1=='CL':
                        #     list[3] = value + way + '!'
                        #     list[19] = extra
                        # if key1=='MG':
                        #     list[7] = value + way + '!'
                        #     list[19] = extra
                if m1_2:
                    for match in m1_2:
                        location1 = int(match.span(1)[0])
                        location2=int(match.span(3)[0])
                        key1=match.group(1).upper()
                        way=match.group(2)
                        extra=match.group(3)
                        if key1 in dict.keys():
                            dict[key1].append([way,location1])
                            #dict['extra'].append(extra)
                        else:
                            dict[key1] = [[way,location1]]
                        dict['extra'].append(extra)
                        # if key1=='LP':
                        #     list[5]=way
                        #     list[19]=extra
                        # if key1=='CL':
                        #     list[3]=way
                        #     list[19] = extra
                        # if key1=='MG':
                        #     list[7]=way
                        #     list[19] = extra
                if m1_3:
                    for match in m1_3:
                        value=match.group(1)
                        location1=int(match.span(1)[0])
                        location2=int(match.span(2)[0])
                        extra=match.group(2)
                        if 'LP0' in dict.keys():
                            dict['LP0'].append([value,location1])
                            #dict['extra'].append(extra)
                        else:
                            dict['LP0'] = [[value,location1]]
                        dict['extra'].append(extra)
                        # list[6]=value
                        # list[19]=extra
            if m2:
                IP=m2.group()
                m2_1=re.match(r'(/|(.{1,30}))',m2.group(1),re.I)
                m2_2 = re.match(r'=([0-9]+)(.{0-30})!(.*)', m2.group(1), re.I)
                m2_3 = re.match(r'=([0-9]+)/(.*)', m2.group(1), re.I)
                dict['extra']=[]
                if m2_1:
                    location = int(m2_1.span(1)[0])
                    message=m2_1.group(1)
                    if 'IP' in dict.keys():
                        dict['IP'].append([message,location])
                    else:
                        dict['IP'] = [[message,location]]
                    # list[2]=message
                if m2_2:
                    location1 = int(m2_2.span(1)[0])
                    location2=int(m2_2.span(3)[0])
                    number=m2_2.group(1)+m2_2.group(2)
                    extra=m2_2.group(3)
                    if 'IP' in dict.keys():
                        dict['IP'].append([number,location1])
                        #dict['extra'].append(extra)
                    else:
                        dict['IP'] = [[number,location1]]
                    dict['extra'].append(extra)
                    # list[2]=number
                    # list[19]=extra
                if m2_3:
                    location1 = int(m2_3.span(1)[0])
                    location2=int(m2_3.span(2)[0])
                    number=m2_3.group(1)
                    extra=m2_3.group(2)
                    if 'IP' in dict.keys():
                        dict['IP'].append([number,location1])
                        #dict['extra'].append(extra)
                    else:
                        dict['IP'] = [[number,location1]]
                    dict['extra'].append(extra)
                    # list[2]=number
                    # list[19]=extra
            if m3:
                extra1=m3.group(1)+':I='+m3.group(2)
                m3_1=re.finditer(r'([a-z]{2,3})=([0-9]+)#(.{0,10})!',m3.group(3),re.I)
                m3_2=re.finditer(r'([a-z]{2,3})(/|#!|#.{0,10}!)',m3.group(3),re.I)
                m3_3=re.finditer(r'PL=([0-9]+)(.*)',m3.group(3),re.I)
                m3_4=re.finditer(r'PP1(.*)',m3.group(3),re.I)
                m3_5=re.finditer(r'XSF=(.{1,30})!',m3.group(3),re.I)
                dict['extra']=[extra1]
                if m3_1:
                    for match in m3_1:
                        location = int(match.span(1)[0])
                        key=match.group(1).upper()
                        message=match.group(2)+'#'+match.group(3)+'!'
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                            #dict['extra']=[extra]
                        # if key=='IP':
                        #     list[2]='#'+message+'!'
                        # if key=='CL':
                        #     list[3]='#'+message+'!'
                        # if key=='SS':
                        #     list[4]='#'+message+'!'
                        # if key=='LP':
                        #     list[5]='#'+message+'!'
                        # if key=='LP0':
                        #     list[6]='#'+message+'!'
                        # if key=='MG':
                        #     list[7]='#'+message+'!'
                        # if key=='DV':
                        #     list[9]='#'+message+'!'
                        # if key=='XSF':
                        #     list[10]='#'+message+'!'
                        # if key=='SF':
                        #     list[11]='#'+message+'!'
                        # if key=='PD':
                        #     list[12]='#'+message+'!'
                if m3_2:
                    for match in m3_2:
                        location = int(match.span(1)[0])
                        key=match.group(1).upper()
                        message=match.group(2)
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                            #dict['extra']=[extra]
                        # if key=='IP':
                        #     list[2]=message
                        # if key=='CL':
                        #     list[3]=message
                        # if key=='SS':
                        #     list[4]=message
                        # if key=='LP':
                        #     list[5]=message
                        # if key=='LP0':
                        #     list[6]=message
                        # if key=='MG':
                        #     list[7]=message
                        # if key=='DV':
                        #     list[9]=message
                        # if key=='XSF':
                        #     list[10]=message
                        # if key=='SF':
                        #     list[11]=message
                        # if key=='PD':
                        #     list[12]=message
                if m3_3:
                    for match in m3_3:
                        location1 = int(match.span(1)[0])
                        location2=int(match.span(2)[0])
                        message=match.group(1)
                        extra=match.group(2)
                        if '#' in extra:
                            pass
                        else:
                            m3_3_1=re.finditer(r'([a-z]+)=(.{0,8})!',match.group(2),re.I)
                            if 'PL' in dict.keys():
                                dict['PL'].append([message,location1])
                                #dict['extra'].append(extra2)
                            else:
                                dict['PL'] = [[message,location1]]
                            for match in m3_3_1:
                                key_word=match.group(1)
                                if key_word=='PL':
                                    dict['PL'].append([match.group(1)+'='+match.group(2)+'!',location1])
                                elif key_word=='XSF':
                                    pass
                                else:
                                    location2=int(match.span(1)[0])
                                    if len(key_word)==1:
                                        key_word='PL'
                                    else:
                                        key_word=key_word
                                    message2=match.group(2)
                                    if key_word in dict.keys():
                                        dict[key_word].append([message2+'!', location2])
                                        # dict['extra'].append(extra2)
                                    else:
                                        dict[key_word] = [[message2+'!', location2]]
                        #dict['extra'].append(extra2)
                        # list[8]=message
                        # list[19]=extra1+' '+extra2

                if m3_4:
                    for match in m3_4:
                        location = int(match.span(1)[0])
                        message=match.group(1)
                        if 'PP1' in dict.keys():
                            dict['PP1'].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict['PP1'] = [[message,location]]
                            #dict['extra']=[extra]
                        #list[16]=message

                if m3_5:
                    for match in m3_5:
                        print(match)
                        location1 = int(match.span(1)[0])
                        #location2 = int(match.span(2)[0])
                        message=match.group(1)
                        #extra2=match.group(2)
                        if 'XSF' in dict.keys():
                            dict['XSF'].append([message+'!',location1])
                            #dict['extra'].append(extra2)
                        else:
                            dict['XSF'] = [[message+'!',location1]]
                        #dict['extra'].append(extra2)
                        # list[10]=message
                        # list[19]=extra1+' '+extra2
            if m4:
                m4_1=re.match(r'=([0-9]+)(.*)',m4.group(1),re.I)
                PL=m4_1.group(1)
                extra=m4_1.group(2)
                location1 = int(m4_1.span(1)[0])
                location2=int(m4_1.span(2)[0])
                if 'PL' in dict.keys():
                    dict['PL'].append([PL,location1])
                    #dict['extra'].append(extra)
                else:
                    dict['PL'] = [[PL,location1]]
                dict['extra'].append(extra)
                # list[8]=PL
                # list[19]=extra
            if m5:
                m5_1=re.match(r'=(.{1,30})!(.*)',m5.group(1),re.I)
                XSF=m5_1.group(1)+'!'
                extra=m5_1.group(2)
                location1 = int(m5_1.span(1)[0])
                location2=int(m5_1.span(2)[0])
                if 'XSF' in dict.keys():
                    dict['XSF'].append([XSF,location1])
                    #dict['extra'].append(extra)
                else:
                    dict['XSF'] = [[XSF,location1]]
                dict['extra'].append(extra)
                # list[10]=XSF
                # list[19]=extra
            if m6:
                SS=m6.group(2)
                #list[4]=SS
                extra=m6.group(1)+':SS='+SS
                #list[19]=extra
                m6_1=re.finditer(r'([a-z]{1,2})(/|=|!)(.*)',m6.group(3),re.I)
                m6_2=re.finditer(r'([a-z]{1,2})#(.{0,30})!',m6.group(3),re.I)
                m6_3=re.finditer(r'LP0=([0-9]+),(.{1,15})(#!|#;)(.*)',m6.group(3),re.I)
                m6_8=re.finditer(r'LP0=(\+|-)([0-9]+),(.{1,15})#;(.{1,10})!',m6.group(3),re.I)
                m6_4=re.finditer(r'HCL=(.{1,8})!(\(was (.{1,8})\)| \(was (.{1,8})\)|)',m6.group(3),re.I)
                m6_5=re.finditer(r'LCL=(.{1,8})!(\(was (.{1,8})\)| \(was (.{1,8})\))',m6.group(3),re.I)
                m6_6 = re.finditer(r'SCL=(.{1,8})!(\(was (.{1,8})\)| \(was (.{1,8})\)|)', m6.group(3), re.I)
                m6_7 = re.finditer(r'XCL=(.{1,8})!(\(was (.{1,8})\)| \(was (.{1,8})\)|)', m6.group(3), re.I)
                dict['extra']=[extra]
                if m6_1:
                    for match in m6_1:
                        location = int(match.span(1)[0])
                        key=match.group(1).upper()
                        if '!' in match.group(2):
                            message=match.group(2)
                        elif '/' in match.group(2):
                            message=match.group(2)
                        else:
                            message=match.group(3)
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                        # if key=='IP':
                        #     list[2]=message
                        # if key=='CL':
                        #     list[3]=message
                        # if key=='SS':
                        #     list[4]=message
                        # if key=='LP':
                        #     list[5]=message
                        # if key=='LP0':
                        #     list[6]=message
                        # if key=='MG':
                        #     list[7]=message
                        # if key=='PL':
                        #     list[8]=message
                        # if key=='DV':
                        #     list[9]=message
                        # if key=='XSF':
                        #     list[10]=message
                        # if key=='SF':
                        #     list[11]=message
                        # if key=='PD':
                        #     list[12]=message
                if m6_2:
                    for match in m6_2:
                        location = int(match.span(1)[0])
                        key=match.group(1).upper()
                        message=match.group(2)
                        if key in dict.keys():
                            dict[key].append(['#'+message+'!',location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [['#'+message+'!',location]]
                            #dict['extra'] = [extra]
                        # if key=='IP':
                        #     list[2]='#'+message+'!'
                        # if key=='CL':
                        #     list[3]='#'+message+'!'
                        # if key=='SS':
                        #     list[4]='#'+message+'!'
                        # if key=='LP':
                        #     list[5]='#'+message+'!'
                        # if key=='LP0':
                        #     list[6]='#'+message+'!'
                        # if key=='MG':
                        #     list[7]='#'+message+'!'
                        # if key=='PL':
                        #     list[8]='#'+message+'!'
                        # if key=='DV':
                        #     list[9]='#'+message+'!'
                        # if key=='XSF':
                        #     list[10]='#'+message+'!'
                        # if key=='SF':
                        #     list[11]='#'+message+'!'
                        # if key=='PD':
                        #     list[12]='#'+message+'!'
                if m6_3:
                    for match in m6_3:
                        message=''
                        location1 = int(match.span(1)[0])
                        location2=int(match.span(3)[0])
                        if '#!' in match.group(3):
                            extra=match.group(4)
                            message = match.group(1) + ',' + match.group(2)+'#!'
                        elif '#;' in match.group(3):
                            extra=''
                            message=match.group(1)+','+match.group(2)+match.group(3)+match.group(4)
                        if extra=='':
                            pass
                        else:
                            dict['extra'].append(extra)
                        #message=match.group(1)+','+match.group(2)
                        if 'LP0' in dict.keys():
                            dict['LP0'].append([message,location1])
                            #dict['extra'].append(extra)
                        else:
                            dict['LP0'] = [[message,location1]]
                            #dict['extra'] = [extra]
                        #list[6]=message
                if m6_4:
                    for match in m6_4:
                        extra11=''
                        location = int(match.span(1)[0])
                        if match.group(2)=='':
                            message = match.group(1)+'!'
                        else:
                            extra11=match.group(2)
                            message=match.group(1)+'!'+extra11
                        if 'HCL' in dict.keys():
                            dict['HCL'].append([message,location])
                        else:
                            dict['HCL'] = [[message,location]]

                        if 'CL' in dict.keys():
                            del dict['CL']

                if m6_5:
                    for match in m6_5:
                        extra11 = ''
                        location = int(match.span(1)[0])
                        if match.group(2) == '':
                            message = match.group(1)+'!'
                        else:
                            extra11=match.group(2)
                            message = match.group(1) + '!' + extra11
                        if 'LCL' in dict.keys():
                            dict['LCL'].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict['LCL'] = [[message,location]]
                        if 'CL' in dict.keys():
                            del dict['CL']
                if m6_6:
                    for match in m6_6:
                        extra11 = ''
                        location = int(match.span(1)[0])
                        if match.group(2) == '':
                            message = match.group(1)+'!'
                        else:
                            extra11=match.group(2)
                            message = match.group(1) + '!' + extra11
                        if 'SCL' in dict.keys():
                            dict['SCL'].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict['SCL'] = [[message,location]]
                        if 'CL' in dict.keys():
                            del dict['CL']
                if m6_7:
                    for match in m6_7:
                        extra11 = ''
                        location = int(match.span(1)[0])
                        if match.group(2) == '':
                            message = match.group(1)+'!'
                        else:
                            extra11=match.group(2)
                            message = match.group(1) + '!' + extra11
                        if 'XCL' in dict.keys():
                            dict['XCL'].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict['XCL'] = [[message,location]]
                        if 'CL' in dict.keys():
                            del dict['CL']
                if m6_8:
                    for match in m6_8:
                        key='LP0'
                        location=int(match.span(1)[0])
                        message=match.group(1)+match.group(2)+','+match.group(3)+'#;'+match.group(4)+'!'
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
            if m7:
                extra='KEY='+m7.group(1)+' I='+m7.group(2)
                m7_1=re.finditer(r'([a-z]+)/(.*)',m7.group(3),re.I)
                m7_2=re.finditer(r'([a-z]+)=([0-9]+)#;(.{1,15})!(.*)',m7.group(3),re.I)
                m7_3 = re.finditer(r'([a-z]+);(.*)', m7.group(3), re.I)
                m7_4 = re.finditer(r'PL=([0-9]+)(.*)', m7.group(3), re.I)
                m7_5 = re.finditer(r'([a-z]+)=([0-9]+)/(.*)', m7.group(3), re.I)
                m7_6 = re.finditer(r'XSF=(\+|-)([0-9]+)(.{0,15})!(.*)', m7.group(3), re.I)
                m7_7 = re.finditer(r'XSF=(\+|-)([0-9]+)/(.*)', m7.group(3), re.I)
                m7_8 = re.finditer(r'IP=([0-9]+)!(.*)',m7.group(3),re.I)
                m7_9 = re.finditer(r'PP([0-9]+)=(.*)', m7.group(3), re.I)
                m7_10 = re.finditer(r'PP([0-9]+)#(.*)', m7.group(3), re.I)
                dict['extra']=extra
                if m7_10:
                    for match in m7_10:
                        location=int(match.span(1)[0])
                        key='PP'
                        message=match.group(1)+',#'+match.group(2)
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m7_9:
                    for match in m7_9:
                        location=int(match.span(1)[0])
                        key='PP'
                        message=match.group(1)+','+match.group(2)
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m7_8:
                    for match in m7_8:
                        location=int(match.span(1)[0])
                        key='IP'
                        message=match.group(1)+'!'+match.group(2)
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m7_7:
                    for match in m7_7:
                        location = int(match.span(1)[0])
                        key='XSF'
                        message=match.group(1)+match.group(2)+'/'+match.group(3)
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m7_6:
                    for match in m7_6:
                        location = int(match.span(1)[0])
                        key='XSF'
                        message=match.group(1)+match.group(2)+match.group(3)+'!'+match.group(4)
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m7_5:
                    for match in m7_5:
                        location=int(match.span(1)[0])
                        key=match.group(1).upper()
                        message=match.group(2)+'/'+match.group(3)
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m7_4:
                    for match in m7_4:
                        location1 = int(match.span(1)[0])
                        location2=int(match.span(2)[0])
                        message=match.group(1)
                        extra=match.group(2)
                        if '#;' in extra:
                            pass
                        else:
                            if 'PL' in dict.keys():
                                dict['PL'].append([message, location1])
                                # dict['extra'].append(extra2)
                            else:
                                dict['PL'] = [[message, location1]]
                            m3_3_1=re.finditer(r'([a-z]+)=(.{0,15})!',match.group(2),re.I)
                            if m3_3_1:
                                for match in m3_3_1:
                                    location2=int(match.span(1)[0])
                                    message=match.group(1)+'='+match.group(2)+'!'
                                    if 'PL' in dict.keys():
                                        dict['PL'].append([message,location2])
                                        #dict['extra'].append(extra2)
                                    else:
                                        dict['PL'] = [[message,location2]]
                        #dict['extra'].append(extra2)
                        # list[8]=message
                        # list[19]=extra1+' '+extra2
                if m7_1:
                    for match in m7_1:
                        key=match.group(1).upper()
                        location=int(match.span(1)[0])
                        message='/'+match.group(2)
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m7_2:
                    for match in m7_2:
                        key = match.group(1)
                        location=int(match.span(1)[0])
                        message=match.group(2)+'#;'+match.group(3)+'!'+match.group(4)
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m7_3:
                    for match in m7_3:
                        key=match.group(1)
                        location=int(match.span(1)[0])
                        message=';'+match.group(2)
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
            if m8:
                extra='KEY='+m8.group(1)+' '+m8.group(2)+'='+m8.group(3)
                dict['extra']=extra
                m8_1 = re.finditer(r'(SA|SI)=([0-9]+)', m8.group(4), re.I)
                m8_2 = re.finditer(r'([a-z]{2,2})=(.{1,15})! \((.{1,25})\)', m8.group(4), re.I)
                m8_3 = re.finditer(r'([a-z]{1})(#|\^)=(.{1,15})! \((.{1,15})\)', m8.group(4), re.I)
                m8_4 = re.finditer(r'([a-z]{2,2})([0-9]{1,4})=([0-9]+)! \((.{1,25})\)', m8.group(4), re.I)
                if m8_1:
                    for match in m8_1:
                        key=match.group(1)
                        location=int(match.span(1)[0])
                        message=match.group(2)
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m8_2:
                    for match in m8_2:
                        print(match)
                        key=match.group(1)
                        location=int(match.span(1)[0])
                        message=match.group(2)+'!'+'('+match.group(3)+')'
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m8_3:
                    for match in m8_3:
                        print(match)
                        key=match.group(1)+match.group(2)
                        location=int(match.span(1)[0])
                        message=match.group(3)+'!'+' ('+match.group(4)+')'
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m8_4:
                    for match in m8_4:
                        key=match.group(1)+match.group(2)
                        location=int(match.span(1)[0])
                        message=match.group(3)+'!'+'('+match.group(4)+')'
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
            if m9:
                extra='KEY='+m9.group(1)
                dict['extra']=extra
                key1=m9.group(2)+m9.group(3)
                location=m9.group(2)
                message=m9.group(4)+'! '+'('+m9.group(5)+')'
                if key1 in dict.keys():
                    dict[key1].append([message, location])
                    # dict['extra'].append(extra)
                else:
                    dict[key1] = [[message, location]]
                m9_1 = re.finditer(r'(SA|SI)=([0-9]+)', m9.group(6), re.I)
                m9_2 = re.finditer(r'([a-z]{2,2})=(.{1,15})! \((.{1,25})\)', m9.group(6), re.I)
                m9_3 = re.finditer(r'([a-z]{1})(#|\^)=(.{1,15})! \((.{1,15})\)', m9.group(6), re.I)
                m9_4 = re.finditer(r'([a-z]{2,2})([0-9]{1,4})=([0-9]+)! \((.{1,25})\)', m9.group(6), re.I)
                if m9_1:
                    for match in m9_1:
                        key=match.group(1)
                        location=int(match.span(1)[0])
                        message=match.group(2)
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m9_2:
                    for match in m9_2:
                        print(match)
                        key=match.group(1)
                        location=int(match.span(1)[0])
                        message=match.group(2)+'!'+'('+match.group(3)+')'
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m9_3:
                    for match in m9_3:
                        print(match)
                        key=match.group(1)+match.group(2)
                        location=int(match.span(1)[0])
                        message=match.group(3)+'!'+'('+match.group(4)+')'
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
                if m9_4:
                    for match in m9_4:
                        key=match.group(1)+match.group(2)
                        location=int(match.span(1)[0])
                        message=match.group(3)+'!'+'('+match.group(4)+')'
                        if key in dict.keys():
                            dict[key].append([message,location])
                            #dict['extra'].append(extra)
                        else:
                            dict[key] = [[message,location]]
            if m10:
                extra='Term'+m10.group(1)
                dict['extra']=extra
            if m11:
                SS = m11.group(2)
                # list[4]=SS
                extra ='KEY='+m11.group(1) + ':SS=' + SS
                # list[19]=extra
                m11_1 = re.finditer(r'([a-z]{1,2})(/|=|!)(.*)', m11.group(3), re.I)
                m11_2 = re.finditer(r'([a-z]{1,2})#(.{0,30})!', m11.group(3), re.I)
                m11_3 = re.finditer(r'LP0=([0-9]+),(.{1,15})(#!|#;)(.*)', m11.group(3), re.I)
                m11_4 = re.finditer(r'HCL=(.*)', m11.group(3), re.I)
                m11_5 = re.finditer(r'LCL=(.*)', m11.group(3), re.I)
                m11_6 = re.finditer(r'SCL=(.*)', m11.group(3), re.I)
                m11_7 = re.finditer(r'XCL=(.*)', m11.group(3), re.I)
                m11_8=re.finditer(r'LP0=([0-9]+)#;(.{1,15})!(.*)', m11.group(3), re.I)
                m11_9 = re.finditer(r'LP0(/)(.*)', m11.group(3), re.I)
                m11_10 = re.finditer(r'LP0=([0-9]+)!(.*)', m11.group(3), re.I)
                m11_11 = re.finditer(r'LP([0-9]+)=(\+|-|)([0-9]+),(\+|-|)([0-9]+)\^([a-z]+)([0-9]+)! \(was (\+|-|)([0-9]+),(\+|-|)([0-9]+)\^([a-z]+)([0-9]+)\)', m11.group(3), re.I)
                m11_12 = re.finditer(r'LP0=(\+|-)([0-9]+),(.{1,15})#;(.{1,15})!', m11.group(3), re.I)
                m11_13=re.finditer(r'LP0=([0-9]+),([0-9]+)\^([a-z]+)([0-9]+)!',m11.group(3),re.I)
                dict['extra'] = [extra]
                if m11_13:
                    for match in m11_13:
                        location = int(match.span(1)[0])
                        key = 'LP0'
                        message=match.group(1)+','+match.group(2)+'^'+match.group(3)+match.group(4)+'!'
                        if key in dict.keys():
                            dict[key].append([message, location])
                            # dict['extra'].append(extra)
                        else:
                            dict[key] = [[message, location]]
                if m11_12:
                    for match in m11_12:
                        location=int(match.span(1)[0])
                        key='LP0'
                        message=match.group(1)+match.group(2)+','+match.group(3)+'#;'+match.group(4)+'!'
                        if key in dict.keys():
                            dict[key].append([message, location])
                            # dict['extra'].append(extra)
                        else:
                            dict[key] = [[message, location]]
                if m11_11:
                    for match in m11_11:
                        location=int(match.span(1)[0])
                        key='LP'+match.group(1)
                        message=match.group(2)+match.group(3)+','+match.group(4)+match.group(5)+'^'+match.group(6)+match.group(7)+'!(was'+match.group(8)+match.group(9)+','+match.group(10)+match.group(11)+''+match.group(12)+match.group(13)+')'
                        if key in dict.keys():
                            dict[key].append([message, location])
                            # dict['extra'].append(extra)
                        else:
                            dict[key] = [[message, location]]
                if m11_10:
                    for match in m11_10:
                        location = int(match.span(1)[0])
                        key='LP0'
                        message=match.group(1)+'!'+match.group(2)
                        if key in dict.keys():
                            dict[key].append([message, location])
                            # dict['extra'].append(extra)
                        else:
                            dict[key] = [[message, location]]
                if m11_9:
                    for match in m11_9:
                        location = int(match.span(1)[0])
                        key = 'LP0'
                        message='/'+match.group(2)
                        if key in dict.keys():
                            dict[key].append([message, location])
                            # dict['extra'].append(extra)
                        else:
                            dict[key] = [[message, location]]
                if m11_8:
                    for match in m11_8:
                        location = int(match.span(1)[0])
                        key='LP0'
                        message=match.group(1)+'#;'+match.group(2)+'!'+match.group(3)
                        if key in dict.keys():
                            dict[key].append([message, location])
                            # dict['extra'].append(extra)
                        else:
                            dict[key] = [[message, location]]
                if m11_1:
                    for match in m11_1:
                        location = int(match.span(1)[0])
                        key = match.group(1).upper()
                        extra=match.group(3)
                        if extra==' (Activated)' or extra==' (Timed - removed by system)':
                            extra=extra
                        else:
                            extra=''
                        if '!' in match.group(2):
                            message = match.group(2)
                        elif '/' in match.group(2):
                            message = match.group(2)
                        else:
                            message = match.group(3)
                        if key in dict.keys():
                            dict[key].append([message+extra, location])
                            # dict['extra'].append(extra)
                        else:
                            dict[key] = [[message+extra, location]]
                        # if key=='IP':
                        #     list[2]=message
                        # if key=='CL':
                        #     list[3]=message
                        # if key=='SS':
                        #     list[4]=message
                        # if key=='LP':
                        #     list[5]=message
                        # if key=='LP0':
                        #     list[6]=message
                        # if key=='MG':
                        #     list[7]=message
                        # if key=='PL':
                        #     list[8]=message
                        # if key=='DV':
                        #     list[9]=message
                        # if key=='XSF':
                        #     list[10]=message
                        # if key=='SF':
                        #     list[11]=message
                        # if key=='PD':
                        #     list[12]=message
                if m11_2:
                    for match in m11_2:
                        location = int(match.span(1)[0])
                        key = match.group(1).upper()
                        message = match.group(2)
                        if key in dict.keys():
                            dict[key].append(['#' + message + '!', location])
                            # dict['extra'].append(extra)
                        else:
                            dict[key] = [['#' + message + '!', location]]
                            # dict['extra'] = [extra]
                        # if key=='IP':
                        #     list[2]='#'+message+'!'
                        # if key=='CL':
                        #     list[3]='#'+message+'!'
                        # if key=='SS':
                        #     list[4]='#'+message+'!'
                        # if key=='LP':
                        #     list[5]='#'+message+'!'
                        # if key=='LP0':
                        #     list[6]='#'+message+'!'
                        # if key=='MG':
                        #     list[7]='#'+message+'!'
                        # if key=='PL':
                        #     list[8]='#'+message+'!'
                        # if key=='DV':
                        #     list[9]='#'+message+'!'
                        # if key=='XSF':
                        #     list[10]='#'+message+'!'
                        # if key=='SF':
                        #     list[11]='#'+message+'!'
                        # if key=='PD':
                        #     list[12]='#'+message+'!'
                if m11_3:
                    for match in m11_3:
                        message = ''
                        location1 = int(match.span(1)[0])
                        location2 = int(match.span(3)[0])
                        if '#!' in match.group(3):
                            extra = match.group(4)
                            message = match.group(1) + ',' + match.group(2) + '#!'
                        elif '#;' in match.group(3):
                            extra = ''
                            message = match.group(1) + ',' + match.group(2) + match.group(3) + match.group(4)
                        if extra == '':
                            pass
                        else:
                            dict['extra'].append(extra)
                        # message=match.group(1)+','+match.group(2)
                        if 'LP0' in dict.keys():
                            dict['LP0'].append([message, location1])
                            # dict['extra'].append(extra)
                        else:
                            dict['LP0'] = [[message, location1]]
                            # dict['extra'] = [extra]
                        # list[6]=message
                if m11_4:
                    for match in m11_4:
                        location = int(match.span(1)[0])
                        message = match.group(1)
                        if 'HCL' in dict.keys():
                            dict['HCL'].append([message, location])
                            # dict['extra'].append(extra)
                        else:
                            dict['HCL'] = [[message, location]]
                            # dict['extra'] = [extra]
                        # list[17]=message
                if m11_5:
                    for match in m11_5:
                        location = int(match.span(1)[0])
                        message = match.group(1)
                        if 'LCL' in dict.keys():
                            dict['LCL'].append([message, location])
                            # dict['extra'].append(extra)
                        else:
                            dict['LCL'] = [[message, location]]
                if m11_6:
                    for match in m11_6:
                        location = int(match.span(1)[0])
                        message = match.group(1)
                        if 'SCL' in dict.keys():
                            dict['SCL'].append([message, location])
                            # dict['extra'].append(extra)
                        else:
                            dict['SCL'] = [[message, location]]
                if m11_7:
                    for match in m11_7:
                        location = int(match.span(1)[0])
                        message = match.group(1)
                        if 'XCL' in dict.keys():
                            dict['XCL'].append([message, location])
                            # dict['extra'].append(extra)
                        else:
                            dict['XCL'] = [[message, location]]
                if m11_8:
                    for match in m11_8:
                        key = 'LP0'
                        location = int(match.span(1)[0])
                        message = match.group(1) + match.group(2) + ',' + match.group(3) + '#;' + match.group(4) + '!'
                        if key in dict.keys():
                            dict[key].append([message, location])
                            # dict['extra'].append(extra)
                        else:
                            dict[key] = [[message, location]]

        for key in dict.keys():
            aa=key
            bb=dict[key]
            if 'DW' in aa:
                if len(aa)>2:
                    del dict[key]
                    t=str(aa)[0]
                    bb.append(t)
                    dict['DW']=bb
            #排序操作
        for key in dict:
            if key=='extra':
                pass
            elif key=='DW':
                pass
            else:
                 l=len(dict[key])
                 print(dict[key])
                 if len(dict[key])>1:
                     for i in range(0,len(dict[key])):
                         for j in range(i,len(dict[key])):
                             if int(dict[key][i][1])>int(dict[key][j][1]):
                                 mid=dict[key][i]
                                 dict[key][i]=dict[key][j]
                                 dict[key][j]=mid
                             else:
                                pass
                 else:
                     pass
        print(dict)
        for key in dict:
            lll=dict[key]
            if key=='extra':
                pass
            else:
                dd=[]
                l=len(dict[key])
                for i in range(0,l):
                    k=''
                    k=dict[key][i][0]
                    print(k)
                    dd.append(dict[key][i][0])
                dict[key]=dd
        if 'CL' in dict.keys():
            if 'HCL' in dict.keys():
                if dict['CL'] == dict['HCL']:
                    del dict['CL']
            if 'XCL' in dict.keys():
                if dict['CL'] == dict['XCL']:
                    del dict['CL']
            if 'SCL' in dict.keys():
                if dict['CL'] == dict['SCL']:
                    del dict['CL']
            if 'LCL' in dict.keys():
                if dict['CL'] == dict['LCL']:
                    del dict['CL']
        elif 'LAN' in dict.keys():
            del dict['LAN']
        else:
            pass
        #m6_8多出来的错误匹配删除
        if 'X' in dict.keys():
            del dict['X']

        aaaa=[]
        for key in dict.keys():
            aaaa.append(key)

        #类型匹配
        if 'PL' in aaaa:
            message1='Split,'
        else:
            message1=''
        if 'IP' in aaaa:
            message2='Split,'
        else:
            message2=''
        if 'SP' in aaaa:
            message3='Split,'
        else:
            message3=''
        if 'OPD' in aaaa:
            message4='Split,'
        else:
            message4=''
        if 'Plan' in aaaa:
            message15='Split,'
        else:
            message15=''

        if message1!='' or message2!='' or message3!='' or message4!='' or message15!='':
            message111='Split,'
        else:
            message111=''

        if 'CL' in aaaa:
            message5='Cycle,'
        else:
            message5=''
        if 'HCL' in aaaa:
            message6='Cycle,'
        else:
            message6=''
        if 'XCL' in aaaa:
            message7='Cycle,'
        else:
            message7=''
        if 'SCL' in aaaa:
            message8='Cycle,'
        else:
            message8=''
        if 'LCL' in aaaa:
            message9=' Cycle,'
        else:
            message9=''

        if message5!='' or message6!='' or message7!='' or message8!='' or message9!='':
            message222='Cycle,'
        else:
            message222=''

        if 'LP' in aaaa:
            message10='Coordination,'
        else:
            message10=''
        if 'LP0' in aaaa:
            message11='Coordination,'
        else:
            message11=''
        if 'MG' in aaaa:
            message12='Coordination,'
        else:
            message12=''
        if 'DV' in aaaa:
            message13='Coordination,'
        else:
            message13=''

        if message10!='' or message11!='' or message12!='' or message13!='':
            message333='Coordination,'
        else:
            message333=''

        if 'PP' in aaaa:
            message14='PP,'
        else:
            message14=''

        if message14!='':
            message444='PP,'
        else:
            message444=''


        if 'DW' in aaaa:
            message16='Dwell,'
        else:
            message16=''
        if 'XSF' in aaaa:
            message17='XSF,'
        else:
            message17=''

        if message17!='':
            message555='XSF,'
        else:
            message555=''

        if message16!='':
            message999='Dwell,'
        else:
            message999=''


        if 'VF' in aaaa:
            message18='Other,'
        else:
            message18=''
        if 'SD' in aaaa:
            message19='Other,'
        else:
            message19=''
        if 'D#' in aaaa:
            message20='Other,'
        else:
            message20=''

        if message18!='' or message19!='' or message20!='':
            message666='Other,'
        else:
            message666=''

        if 'Activated' in oper_code:
            message21='Activated,'
        else:
            message21=''
        #
        # if 'Timed' in oper_code:
        #     message22='Remove,'
        # else:
        #     message22=''
        #
        if message21!='':
            list[4]=message21
        else:
            list[4] = message999 + message111 + message222 + message333 + message444 + message555 + message666

        if list[4]=='':
            list[4]=None
        else:
            list[4]=str(list[4])[:-1]



        j = json.dumps(dict)
        list[8]=j
                #print(list)
        END.append(list)
    #print(END)
    #print(len(END))
    print(END)
    return END
#连接数据库用这个
def insert_data(data):
    pp=pg_inf
    db = psycopg2.connect(dbname=pp['database'], user=pp['user'], password=pp['password'], host=pp['host'], port=pp['port'])
    cr = db.cursor()
    for i in data:
        print(i)
        sql ="insert into record_data_parsing(oper,meaning,opercode,opertime,opertype,region,siteid,userid,message) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cr.execute(sql, i)
        db.commit()
    cr.close()
    db.close()

def main():
    '''链接数据库读取数据'''
    oracle=OracleUser
    operate_data=''
    endtime=datetime.now()
    starttime=endtime-timedelta(minutes=3)
    try:
        conn = cx_Oracle.connect(oracle)
        # conn=psycopg2.connect(database="signal_specialist",user="postgres",password= "postgres",
        #   host= "192.168.20.46",port="5432")
        cur=conn.cursor()
        cur.execute("select * from GET_MANOPERATION_RECORD where  OPERTIME BETWEEN {0} AND {1}".format(starttime,endtime))
        x=cur.fetchall()
        operate_data = x
        conn.commit()
        cur.close()
        conn.close()
    except cx_Oracle.DatabaseError:
        print('数据库连不上')
    except Exception as e:
        print(e)
    '''本地操作解析'''

    '''后续操作'''
    nowTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dict=seperate(operate_data)
    insert_data(dict)
    print('当前时间为：')
    print(nowTime)
    #dict = pd.DataFrame(dict)
    #dict.to_csv(r'E:\大转移\scats_operate_parsing\data_10_24_parsing.csv', index=0)
    #dict.to_csv(r'E:\大转移\scats_operate_parsing\双休日数据优化.csv', index=0)

    #本地输出


#添加定时器，定时操作
def fun_timer():
    main()
    print("这次的操作完成，下次将在3分钟后进行")
    global timer
    timer=threading.Timer(180,fun_timer)
    timer.start()


if __name__ == '__main__':
    pass
    #main()
    # timer=threading.Timer(1,fun_timer)
    # timer.start()