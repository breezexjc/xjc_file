import psycopg2
import cx_Oracle
# from .database import *
from proj.config.database import *
import re
import pandas as pd

aa=[]

class OperateSample():

    KeyWord = {'IP': '绿信比方案', 'CL': '周期', 'SS': '子系统', 'LP': '协调方案', 'LP0': '0号协调方案', 'MG': '协调关系', 'PL': '绿信比方案',
               'DV': '离婚', 'XSF': '标志位', 'SF': None, 'PD': '主相位', 'NGNS': None, 'NSNG': None,
               'NGNG': None, 'PP1': None, 'HCL': '高周期', 'Plan': None, 'Activated': None,'DW':'定灯','PP':'内部方案'}

    def __init__(self, data):
        self.match_data = data
        self.model = []
    def re_model(self):
        # modl1 =
        pass


class Content:
    Sample = True
    table_name = 'GET_MANOPERATION_RECORD'
    table_name_sample = 'GET_MANOPERATION_RECORD_SAMPLE'
    sql_get_operate = "select * from {0} where OPERTIME> '%s'".format(table_name)
    sql_get_operate_sample = "select * from {0}".format(table_name_sample)
    sql_sample_operate = "select DATETIME,OPER_TIME,USERID,DETAILS from OPERETE_DATA_SAMPLE " \
                         "where (to_char(DATETIME,'yyyy-MM-dd')||' '||OPER_TIME) > '2018-09-01 00:00:00';"
    Sample_M1_2 = 'SS=1 CL/ (Timed - removed by system)'


def call_oracle_operate_data(stime):
    db = Oracle()
    db.dbconn()
    if db.conn:
        cr = db.cr
        try:
            if not Content.Sample:
                cr.execute(Content.sql_get_operate % stime)
            else:
                cr.execute(Content.sql_get_operate)
        except Exception as e:
            print("call_oracle_operate_data:", e)
            return None
        else:
            rows = cr.fetchall()
            return rows
    else:
        print("数据库连失败")
        return None


def cal_time_second(time_interval):

    m = re.search(r'([0-9]+)([HM])', time_interval)
    if m:
        # print("m_time_interval:", m)
        intervar = m.group(1)
        time_unit = m.group(2)
        if time_unit == 'H':
            sec = intervar*60*60
        elif time_unit == 'M':
            sec = intervar * 60
        return sec
    else:
        return None


def resolve_dispose(record):
    record.sort(key=lambda x : x[1])
    # print(record)
    sort_record = [i[0] for i in record]
    return sort_record


def change2hour(hours, minutes):
    min2hour = int(minutes) / 60
    change_hours = int(hours) + min2hour
    return change_hours


def re_match_M1(m1, m_remove, m_active):
    m1_resolve = []
    m1_1 = re.finditer(r'([a-z]+)(#|(=|)#;(.{1,8}))!', m1, re.I)
    # m1_active = re.search(r'(Activated)', m1.group(2), re.I)
    # m1_remove = re.search(r'(Timed - removed by system)', m1.group(2), re.I)
    m1_2 = re.finditer(r'([a-z]+)(|[0-9])(=.{1,2}/|/|=([0-9]+)#;(.{1,8})!)', m1, re.I)
    m1_3 = re.finditer(r'LP([0-9])=([0-9]+),([0-9]+)\^([a-z])([0-9]+)#(|;.{1,8})!', m1, re.I)
    if m1_1:
        for match in m1_1:
            location = match.span(1)[0]
            key_word = match.group(1)
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            symbol = match.group(2)
            if symbol == '#':
                meaning = '永久锁定' + key_word_mean + '；'
            else:
                interval = match.group(4)
                if interval == '*':
                    meaning = '锁定' + key_word_mean + '，时长无限；'
                else:
                    time_interval=''
                    tt = re.finditer(r'#;([0-9]*)(H|M)([0-9]*)(.?)', symbol, re.I)
                    if tt:
                        for match in tt:
                            if match.group(2) == 'M' or match.group(2) == 'm':
                                h = str(int(int(match.group(1)) / 60))
                                m = str(int(int(match.group(1)) % 60))
                                time_interval = h + ':' + m + ':00'
                            elif match.group(2) == 'h' or match.group(2) == 'H':
                                if match.group(4) == 'M' or match.group(4) == 'm':
                                    h = str(int(int(match.group(1))) + int(int(match.group(3)) / 60))
                                    m = str(int(int(match.group(3)) % 60))
                                    time_interval = h + ':' + m + ':00'
                                else:
                                    time_interval = match.group(1) + ':00:00'
                    meaning = '锁定' + key_word_mean + '，时长' + time_interval + ';'
            m1_resolve.append([meaning, location])
    if m1_2:
        for match in m1_2:
            # print(match)
            location = match.span(1)[0]
            key_word = match.group(1)
            symbol = match.group(3)
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            if ('/' in symbol):
                if m_remove is None:
                    meaning = key_word_mean + "解除锁定;"
                    m1_resolve.append([meaning, location])
                else:
                    meaning = key_word_mean + "锁定到期，自动解锁;"
                    m1_resolve.append([meaning, location])
                    pass

            else:
                time_interval=''
                value = match.group(4)
                interval = match.group(5)
                if interval == r'*':
                    meaning = value+ '号'+ key_word_mean + ",时长无限;"
                else:
                    tt = re.finditer(r'#;([0-9]*)(H|M)([0-9]*)(.?)', symbol, re.I)
                    if tt:
                        for match in tt:
                            if match.group(2) == 'M' or match.group(2) == 'm':
                                h = str(int(int(match.group(1)) / 60))
                                m = str(int(int(match.group(1)) % 60))
                                time_interval = h + ':' + m + ':00'
                            elif match.group(2) == 'h' or match.group(2) == 'H':
                                if match.group(4) == 'M' or match.group(4) == 'm':
                                    h = str(int(int(match.group(1))) + int(int(match.group(3)) / 60))
                                    m = str(int(int(match.group(3)) % 60))
                                    time_interval = h + ':' + m + ':00'
                                else:
                                    time_interval = match.group(1) + ':00:00'
                    meaning = value+ '号'+ key_word_mean + ",时长" + time_interval + ";"
                m1_resolve.append([meaning, location])
    if m1_3:
        for match in m1_3:
            time_interval=''
            lp_no = match.group(1)
            high_offset = match.group(2)
            low_offset = match.group(3)
            phase = match.group(4)
            link_siteid = match.group(5)
            interval = match.group(6)
            location = match.span(1)[0]
            tt = re.finditer(r'#;([0-9]*)(H|M)([0-9]*)(.?)', interval, re.I)
            if tt:
                for match in tt:
                    if match.group(2) == 'M' or match.group(2) == 'm':
                        h = str(int(int(match.group(1)) / 60))
                        m = str(int(int(match.group(1)) % 60))
                        time_interval = h + ':' + m + ':00'
                    elif match.group(2) == 'h' or match.group(2) == 'H':
                        if match.group(4) == 'M' or match.group(4) == 'm':
                            h = str(int(int(match.group(1))) + int(int(match.group(3)) / 60))
                            m = str(int(int(match.group(3)) % 60))
                            time_interval = h + ':' + m + ':00'
                        else:
                            time_interval = match.group(1) + ':00:00'
            meaning = lp_no + '号协调方案，内容:' + '关联路口：'+link_siteid+';关联相位:'+phase+';高周期相位差：'+high_offset +\
                      ';低周期相位差：' + low_offset + ';时长'+time_interval +';'
            m1_resolve.append([meaning, location])
    if m_active:
        location = m_active.span(1)[0]
        meaning = "操作生效;"
        m1_resolve.append([meaning, location])
    # if m_remove:
    #     location = m_remove.span(1)[0]
    #     meaning = "锁定到期，自动解锁;"
    #     m1_resolve.append([meaning, location])
    if len(m1_resolve) > 0:
        return m1_resolve
    else:
        m1_resolve.append([m1 + 'm1翻译失败；', 1])
        return m1_resolve

def re_match_M0(m0, m_remove, m_active):
    m0_resolve = []

    # print("correct_operate")
    # re.finditer 返回所有匹配结果的迭代结果
    #m0_1 = re.finditer(r'([a-z]+)=([0-9]+)#(|;.{1,4}[HM])!', m0, re.I)
    m0_1 = re.finditer(r'([a-z]+)=([0-9]+)#(.{0,1})!', m0, re.I)
    m0_4= re.finditer(r'([a-z]+)=([0-9]+)#;(.{1,15})', m0, re.I)
    # m0_2 = re.search(r'I=(.*) ', m0.group(3), re.I)
    m0_2 = re.finditer(r'([a-z]+)/', m0, re.I)
    m0_3 = re.finditer(r'([a-z]+)#(|;.{1,4}[HM])!', m0, re.I)
    m0_5=re.finditer(r'Plan=([0-9]+)!(.*)',m0,re.I)
    m0_6 = re.finditer(r'Plan ([0-9]+)!(.*)', m0, re.I)
    m0_1_resolve, m0_2_resolve, m0_3_resolve = ([], [], [])
    # print("m0_3:", m0_3)
    if m0_6:
        for match in m0_6:
            location=match.span(1)[0]
            number=match.group(1)
            message11 = '采用'+number + '号协调方案'
            if match.group(2)=='':
                message=number+'号协调方案'
                m0_resolve.append([message, location])
            else:
                m0_resolve.append([message11, location])
                m0_6_1=re.finditer(r'([a-z]+)=0PD([a-z]{1})',match.group(2),re.I)
                m0_6_2 = re.finditer(r'([a-z]+)=([0-9]+)(NSNG|NGNS|NGNG|NSNS)([a-z]{1})', match.group(2), re.I)
                if m0_6_1:
                    for match in m0_6_1:
                        location=match.span(1)[0]
                        phase1=match.group(1)
                        phase2=match.group(2)
                        message=',设定主相位为'+phase1+',0号绿信比方案,下一相位为'+phase2+','
                        m0_resolve.append([message, location])
                if m0_6_2:
                    for match in m0_6_2:
                        location = match.span(1)[0]
                        phase1 = match.group(1)
                        phase2 = match.group(4)
                        time=match.group(2)
                        message=',设定'+phase1+'相位绿信比为'+time+',不能提前结束，不能被跳过，下一相位为'+phase2
                        m0_resolve.append([message, location])
    if m0_5:
        for match in m0_5:
            location=match.span(1)[0]
            number=match.group(1)
            message11 = '采用'+number + '号协调方案'
            if match.group(2)=='':
                message=number+'号协调方案'
                m0_resolve.append([message, location])
            else:
                m0_resolve.append([message11, location])
                m0_5_1=re.finditer(r'([a-z]+)=0PD([a-z]{1})',match.group(2),re.I)
                m0_5_2 = re.finditer(r'([a-z]+)=([0-9]+)(NSNG|NGNS|NGNG|NSNS)([a-z]{1})', match.group(2), re.I)
                if m0_5_1:
                    for match in m0_5_1:
                        location=match.span(1)[0]
                        phase1=match.group(1)
                        phase2=match.group(2)
                        message=',设定主相位为'+phase1+',0号绿信比方案,下一相位为'+phase2+','
                        m0_resolve.append([message, location])
                if m0_5_2:
                    for match in m0_5_2:
                        location = match.span(1)[0]
                        phase1 = match.group(1)
                        phase2 = match.group(4)
                        time=match.group(2)
                        message=',设定'+phase1+'相位绿信比为'+time+',不能提前结束，不能被跳过，下一相位为'+phase2
                        m0_resolve.append([message, location])
    if m0_4:
        for match in m0_4:
            location = match.span(1)[0]
            key_word=match.group(1)
            key_word_mean=OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean=key_word
            time=match.group(3)
            way=match.group(2)
            tt=re.finditer(r'([0-9]+)(h|m)([0-9]*)(.?)',time,re.I)
            if tt:
                for match in tt:
                    if match.group(2)=='M' or match.group(2)=='m':
                        h=str(int(int(match.group(1))/60))
                        m=str(int(int(match.group(1))%60))
                        time=h+':'+m+':00'
                    elif match.group(2)=='h' or match.group(2)=='H':
                        if match.group(4)=='M' or match.group(4)=='m':
                            h=str(int(int(match.group(1)))+int(int(match.group(3))/60))
                            m=str(int(int(match.group(3))%60))
                            time=h+':'+m+':00'
                        else:
                            time= match.group(1)+':00:00'
            message=key_word_mean+'方案为'+way+'锁定，时长为'+time
            m0_resolve.append([message, location])
    if m0_1:
        for match in m0_1:
            # print(match)
            key_word = match.group(1)
            location = match.span(1)[0]
            plan_no = match.group(2)
            time_interval = match.group(3)
            # print(match.span(1))
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            if key_word_mean=='Plan':
                key_word_mean='绿信比方案'
            if time_interval:
                """是否定时任务"""
                oper_interval = cal_time_second(time_interval)
                # print(siteid+" 路口"+key_word_mean+"设定为 "+pl_no+",持续"+time_interval[1:])
                m0_resolve.append([key_word_mean + "设定为 " + plan_no + ",持续" + time_interval[1:] , location])
            else:
                # print(siteid+" 路口"+key_word+"号日程方案设定为 " + pl_no)
                m0_resolve.append([key_word_mean + "设定为 " + plan_no, location])
    if m0_2:
        for match in m0_2:
            key_word = match.group(1)
            location = match.span(1)[0]
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            # print(siteid+" 路口"+old_plan_no+"号日程方案"+key_word+"解锁")
            m0_resolve.append([key_word_mean + "解锁; ", location])
    if m0_3:
        for match in m0_3:
            location = match.span(1)[0]
            key_word = match.group(1)
            interval = match.group(2)
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            if len(interval) > 0:
                meaning = '建立' + key_word_mean+'，时长' +interval +';'
            else:
                meaning = '建立' + key_word_mean
            m0_resolve.append([meaning, location])

    if len(m0_resolve) > 0:
        return m0_resolve
    else:
        m0_resolve.append([m0 + 'm0 翻译失败；', 1])
        return m0_resolve

def resolve_M2(m2, m_remove ,m_active):
    m2_resolve = []
    m2_0 = re.match(r'(/)', m2, re.I)
    m2_1 = re.finditer(r'=([0-9]+)(!|/|#;(.{1,8})!)', m2, re.I)

    if m2_0:
        location = m2_0.span(1)[0]
        meaning = '绿信比方案解除锁定；'
        m2_resolve.append([meaning, location])
    if m2_1:
        for match in m2_1:
            location = match.span(1)[0]
            ip_no = match.group(1)
            symbol = match.group(2)
            meaning_start = ip_no + '号绿信比方案'
            if symbol == '!':
                meaning = '临时锁定；'
            elif symbol == '/':
                if m_remove:
                    meaning = '锁定到期，自动解锁；'
                else:
                    meaning = '解锁'
            else:
                interval = match.group(3)
                m2_1_interval = re.match(r'([0-9]+):([0-9]+):[0-9]+]', interval)
                if m2_1_interval:
                    hours = m2_1_interval.group(1)
                    minutes = m2_1_interval.group(2)
                    change_hours = change2hour(hours,minutes)
                    meaning = '定时锁定，时长'+ str(change_hours) + '小时；'
                elif interval == '*':
                    meaning = '永久锁定；'
                else:
                    meaning = '定时锁定，时长' + interval + '；'
            m2_resolve.append([meaning_start + meaning, location])
    if m_active:
        location = m_active.span(1)[0]
        meaning = '操作生效;'
        m2_resolve.append([meaning, location])
    if len(m2_resolve) > 0:
        return m2_resolve
    else:
        m2_resolve.append([m2 + 'm2翻译失败；', 1])
        return m2_resolve

def resolve_M3(m3, m_remove ,m_active):
    m3_resolve = []
    m3_1 = re.search(r'([a-z]+.)=([0-9]+)#(|;(.{1,8}))!', m3, re.I)
    m3_2 = re.finditer(r'([a-z]+)/', m3, re.I)
    m3_3 = re.search(r'PL=([0-9]+)(.*)', m3, re.I)
    m3_4 = re.search(r'PP1(.*)',m3,re.I)
    m3_5 = re.finditer(r'!XSF=(\+|-)([0-9]+)!',m3,re.I)
    # m3_3 = re.search(r'PL=([0-9]+)(.*)', m3, re.I)
    if m3_5:
        for match in m3_5:
            location=match.span(1)
            flag=match.group(1)
            number=match.group(2)
            if flag=='-':
                message='关闭'+number+'号标志位'
            else:
                message = '开启' + number + '号标志位'
            m3_resolve.append([message, location])
    if m3_1:
        location = m3_1.span(1)
        key_word = m3_1.group(1)
        plan_no = m3_1.group(2)
        interval = m3_1.group(3)
        key_word_mean = OperateSample.KeyWord.get(key_word.upper())
        if not key_word_mean:
            key_word_mean = key_word
        if len(interval) > 0:
            time_interval = m3_1.group(4)
            tt=re.finditer(r'([0-9]*)(H|M)([0-9]*)(.?)',time_interval,re.I)
            if tt:
                for match in tt:
                    if match.group(2)=='M' or match.group(2)=='m':
                        h=str(int(int(match.group(1))/60))
                        m=str(int(int(match.group(1))%60))
                        time_interval=h+':'+m+':00'
                    elif match.group(2)=='h' or match.group(2)=='H':
                        if match.group(4)=='M' or match.group(4)=='m':
                            h=str(int(int(match.group(1)))+int(int(match.group(3))/60))
                            m=str(int(int(match.group(3))%60))
                            time_interval=h+':'+m+':00'
                        else:
                            time_interval= match.group(1)+':00:00'
            time_desc = '，时长' + time_interval +'；'
        else:
            time_desc = '；'
        meaning = plan_no+'号'+ key_word_mean+'锁定' + time_desc
        m3_resolve.append([meaning, location])
    if m3_2:
        for match in m3_2:
            location = match.span(1)
            key_word = match.group(1)
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            meaning = key_word_mean + '解除锁定'
            m3_resolve.append([meaning, location])
    if m3_3:
        location = m3_3.span(1)
        pl_no = m3_3.group(1)
        meaning_start = pl_no + '号绿信比方案'
        way=m3_3.group(1)
        m3_3_1 = re.search(r' (|X)SF=(.*)', m3_3.group(2), re.I)
        m3_3_2 = re.search(r'([a-z])=.PD([a-z])!',  m3_3.group(2), re.I)
        m3_3_3 = re.finditer(r'([a-z])=([0-9]+)(.{0,4})([a-z])!',  m3_3.group(2), re.I)
        if m3_3_1:
            keyword = m3_3_1.group(1)
            symbol = m3_3_1.group(2)
            meaning = '???'
            if keyword == '':
                if symbol == '!':
                    meaning = way +',号信绿比方案,'+'SF为空，非双周期；'
                else:
                    meaning =way +',号信绿比方案,'+ 'SF为' + symbol + '；'
            elif keyword in('X', 'x'):
                m3_3_1_1 = re.search(r'([+\-])?([0-9]+)(!|#!|/)', symbol)

                if m3_3_1_1:
                    signal = m3_3_1_1.group(1)
                    lamp_no = m3_3_1_1.group(2)
                    symbol1 = m3_3_1_1.group(3)
                    if signal == '+':
                        meaning =way +',号信绿比方案,'+ '开启' +lamp_no+'号标志位；'
                    elif signal == '-' :
                        meaning =way +',号信绿比方案,'+ '关闭' + lamp_no + '号标志位；'
                    elif symbol1 == '/':
                        meaning =way +',号信绿比方案,'+ '解锁' + lamp_no + '号标志位；'
            else:
                meaning =way +',号信绿比方案,'+ '???'
            m3_resolve.append([meaning, location])
        if m3_3_2:
            phase = m3_3_2.group(1)
            next_phase = m3_3_2.group(2)
            meaning =way +',号信绿比方案,'+ '设定' + phase + '相位为主相位，下一相位为' + next_phase + '；'
            m3_resolve.append([meaning, location])
        if m3_3_3:
            for match in m3_3_3:
                location = match.span(1)
                phase = match.group(1)
                phase_length = match.group(2)
                phase_func = match.group(3)
                next_phase=match.group(4)
                if 'NG' in phase_func:
                    func1 = '相位不能提前结束；相位不可以被跳过；'
                else:
                    func1 = ''

                meaning =way +',号信绿比方案,'+ '设定' + phase + '相位绿信比为' + phase_length + ','+func1+'下一相位为'+next_phase
                m3_resolve.append([meaning, location])

    if m3_4:
        location=m3_4.span(1)
        way=m3_4.group(1)
        if '#!' in m3_4.group(1):
            m3_resolve.append(['内部协调方案锁定为1',location])
        if '#;' in m3_4.group(1):
            m3_4_1=re.match(r'#;([0-9]*)([a-z]*)([0-9]*)([a-z]*)',m3_4.group(1),re.I)
            if 'H' in m3_4_1.group(2):
                time1=m3_4_1.group(1)
                if 'M' in m3_4_1.group(4):
                    time2=m3_4_1.group(3)
                    m3_resolve.append(['内部方案锁定为1，时长'+time1+'小时'+time2+'分钟',location])
                else:
                    m3_resolve.append(['内部方案锁定为1，时长'+time1+'小时',location])
            if 'M' in m3_4_1.group(2):
                time1=m3_4_1.group(1)
                m3_resolve.append(['内部方案锁定为1，时长'+time1+'分钟',location])


    if len(m3_resolve) >0:
        return m3_resolve
    else:
        m3_resolve.append([m3+'m3翻译失败；',1])
        return m3_resolve

def resolve_M4(m4, m_remove ,m_active):
    m4_resolve = []
    meaning=[]
    m4_1=re.search(r'=([0-9]+)(.*)',m4,re.I)
    if m4_1:
        if True:
            #for match in m4_1:
            location=m4_1.span(1)
            number=m4_1.group(1)
            m4_1_1=re.findall(r'([a-z]+)=([0-9]+)([a-z]{1,4})([a-z]?)!',m4_1.group(2),re.I)
            mm=m4_1_1
        if len(mm)>=1:
            phase1=m4_1_1[0][0]
            length1=m4_1_1[0][1]
            phase11=m4_1_1[0][3]
            meaning.append(number+'号信绿比方案，'+phase1 +'相位绿信比设定为'+length1+',不能提前结束，不可以跳过,下一相位为'+phase11)
            if len(mm)>=2:
                phase2=m4_1_1[1][0]
                length2=m4_1_1[1][1]
                phase22=m4_1_1[1][3]
                meaning.append(phase2+',相位绿信比设定为'+ length2 +',不能提前结束，不可以跳过,下一相位为'+phase22)
                if len(mm)>=3:
                    phase3 = m4_1_1[2][0]
                    length3 = m4_1_1[2][1]
                    phase33=m4_1_1[2][3]
                    meaning.append(phase3 + ',相位绿信比设定为' + length3 + ',不能提前结束，不可以跳过,下一相位为'+phase33)
                    if len(mm)>=4:
                        phase4 = m4_1_1[3][0]
                        length4 = m4_1_1[3][1]
                        phase44=m4_1_1[3][3]
                        meaning.append(phase4 + ',相位绿信比设定为' + length4 + ',不能提前结束，不可以跳过,下一相位为'+phase44)
                        if len(mm)>=5:
                            phase5 = m4_1_1[4][0]
                            length5 = m4_1_1[4][1]
                            phase55=m4_1_1[4][3]
                            meaning.append(phase5 + ',相位绿信比设定为' + length5 + ',不能提前结束，不可以跳过,下一相位为'+phase55)
                            if len(mm)>=6:
                                phase6 = m4_1_1[5][0]
                                length6 = m4_1_1[5][1]
                                phase66=m4_1_1[6][3]
                                meaning.append(phase6 + ',相位绿信比设定为' + length6 + ',不能提前结束，不可以跳过,下一相位为'+phase66)
                                if len(mm)>=7:
                                    phase7 = m4_1_1[6][0]
                                    length7 = m4_1_1[6][1]
                                    phase77=m4_1_1[6][3]
                                    meaning.append(phase7 + ',相位绿信比设定为' + length7 + ',不能提前结束，不可以跳过,下一相位为'+phase77)
                                else:
                                    pass
                            else:
                                pass
                        else:
                            pass
                    else:
                        pass
                else:
                    pass
            else:
                pass
        else:
            t='信绿比方案为0，原定为双周期，实际为空'
            a=[]
            a.append(t)
            m4_resolve.append([a,location])
            pass
        m4_resolve.append([meaning, location])

    return m4_resolve

def resolve_M5(m5, m_remove, m_active):
    m5_resolve = []
    mm=m5
    m5_1 = re.match(r'=(.)([0-9]+)(.*)', m5, re.I)
    if m5_1:
        location=m5_1.span(1)[0]
        open_close=m5_1.group(1)
        number=m5_1.group(2)
        pp=m5_1.group(3)
        if open_close=="+":
            if 'Activated' in pp:
                m5_resolve.append(['开启'+number+'号标志位，该操作生效。',location])
            else:
                m5_resolve.append(['开启' + number + '号标志位。', location])
        else:
            if 'Activated' in pp:
                m5_resolve.append(['关闭'+number+'号标志位，该操作生效。',location])
            else:
                m5_resolve.append(['关闭' + number + '号标志位。', location])
    return m5_resolve

def resolve_M6(m6, m_remove, m_active):
    m6_resolve = []
    m6_9=re.finditer(r'CL=([0-9]+)\^(;|)(.{0,15})!',m6,re.I)
    m6_8 =re.finditer(r'([a-z]{2,3})!',m6,re.I)
    m6_7 = re.finditer(r'(LCL|SCL|XCL)=(.{1,8})!(\(was (.{1,8})\)| \(was (.{1,8})\)|)',m6,re.I)
    #m6_10 = re.finditer(r'(LCL|SCL|XCL)=(.{1,8})!', m6, re.I)
    m6_4 = re.search(r'HCL=(.{1,8})!(\(was (.{1,8})\)|)', m6, re.I)
    #m6_11 = re.search(r'HCL=(.{1,8})!', m6, re.I)
    #m6_3 = re.search(r'lp0=(.{1,5}),(.)([0-9]+)\^([a-z]+)([0-9]+)#!(.*)', m6, re.I)
    m6_1 = re.search(r'([a-z]+)(/|=([0-9]+)#!|=([0-9]+)#;(.*))', m6, re.I)
    m6_2 = re.search(r'([a-z]+)(#!|#;)(.*)', m6, re.I)
    m6_6 = re.finditer(r'(lp0|LP0)=(|-})([0-9]+),(|-)([0-9]+)\^([a-z]{1})([0-9]+)([a-z]{0,1})(#!|#;)([0-9]{0,10})([a-z]{0,5})',m6,re.I)
    if m6_1:
        #for match in m6_1:
            location=m6_1.span(1)
            key_word=m6_1.group(1)
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            flag=m6_1.group(2)
            if '/' in flag:
                m6_resolve.append([key_word_mean+'锁定解除,',location])
            if '#!' in flag:
                m6_1_1=re.search(r'=([0-9]+)#!',flag,re.I)
                time1=m6_1_1.group(1)
                m6_resolve.append([key_word_mean+'锁定为'+time1,location])
            if '#;' in flag:
                tt = re.finditer(r'([0-9]*)(H|M)([0-9]*)(.?)', flag, re.I)
                aa=re.search(r'=([0-9]+)#;',flag,re.I)
                aa=aa.group(1)
                time_interval=''
                if tt:
                    for match in tt:
                        if match.group(2) == 'M' or match.group(2) == 'm':
                            h = str(int(int(match.group(1)) / 60))
                            m = str(int(int(match.group(1)) % 60))
                            time_interval = h + ':' + m + ':00'
                        elif match.group(2) == 'h' or match.group(2) == 'H':
                            if match.group(4) == 'M' or match.group(4) == 'm':
                                h = str(int(int(match.group(1))) + int(int(match.group(3)) / 60))
                                m = str(int(int(match.group(3)) % 60))
                                time_interval = h + ':' + m + ':00'
                            else:
                                time_interval = match.group(1) + ':00:00'
                        m6_resolve.append([key_word_mean + '锁定为'+aa+',时长为'+time_interval,location])
    if m6_2:
            location=m6_2.span(1)
            key_word=m6_2.group(1)
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            way=m6_2.group(2)
            if '/' in m6_2.group(2):
                m6_resolve.append([key_word_mean+'锁定解除',location])
            if '#!' in m6_2.group(2):
                m6_resolve.append([key_word_mean+'锁定',location])
            if '#;' in m6_2.group(2):
                time_interval=''
                tt = re.finditer(r'([0-9]*)(H|M)([0-9]*)(.?)', m6_2.group(3), re.I)
                if tt:
                    for match in tt:
                        if match.group(2) == 'M' or match.group(2) == 'm':
                            h = str(int(int(match.group(1)) / 60))
                            m = str(int(int(match.group(1)) % 60))
                            time_interval = h + ':' + m + ':00'
                        elif match.group(2) == 'h' or match.group(2) == 'H':
                            if match.group(4) == 'M' or match.group(4) == 'm':
                                h = str(int(int(match.group(1))) + int(int(match.group(3)) / 60))
                                m = str(int(int(match.group(3)) % 60))
                                time_interval = h + ':' + m + ':00'
                            else:
                                time_interval = match.group(1) + ':00:00'
                m6_resolve.append([key_word_mean+'锁定，时长为'+time_interval,location])
    # if m6_3:
    #     location=m6_3.span(1)
    #     port=m6_3.group(5)
    #     phase=m6_3.group(4)
    #     time1=m6_3.group(3)
    #     flag=m6_3.group(2)
    #     if flag=='+':
    #         m6_resolve.append(['锁定0号协调方案，方案内容：比'+ port +'号路口'+phase+'相位晚起点'+time1+'秒。',location])
    #     else:
    #         m6_resolve.append(['锁定0号协调方案，方案内容：比' + port + '号路口' + phase + '相位早起点' + time1 + '秒。', location])
    if m6_4:
        location=m6_4.span(1)
        time1=m6_4.group(1)
        extra=m6_4.group(2)
        if extra=='':
            m6_resolve.append(['最大周期为' + time1, location])
        else:
            tt = re.search(r'\(was (.{1,8})\)', extra, re.I)
            time2 = tt.group(1)
            m6_resolve.append([',原定周期为'+ time2 +',最大周期为'+time1+';',location])
    # if m6_11:
    #     location=m6_11.span(1)
    #     time1=m6_11.group(1)
    #     m6_resolve.append(['最大周期为'+time1,location])
    if m6_6:
        for match in m6_6:
            location=match.span(1)
            port=match.group(7)
            phase=match.group(6)
            flag=match.group(2)
            time1=match.group(3)
            time2=match.group(5)
            long=match.group(9)
            if long=='#!':
                if flag=='+' or '':
                    m6_resolve.append(['锁定0号协调方案，方案内容：比'+port+'号路口'+phase+'相位晚起点'+time1+'-'+time2+'秒',location])
                if flag=='-':
                    m6_resolve.append(['锁定0号协调方案，方案内容：比' + port + '号路口' + phase + '相位早起点' + time1+'-'+time2 + '秒', location])
            elif long=='#;':
                if flag=='+' or '':
                    m6_resolve.append(['锁定0号协调方案('+match.group(10)+match.group(11)+')，方案内容：比'+port+'号路口'+phase+'相位晚起点'+time1+'-'+time2+'秒',location])
                if flag=='-':
                    m6_resolve.append(['锁定0号协调方案('+match.group(10)+match.group(11)+')，方案内容：比' + port + '号路口' + phase + '相位早起点' + time1+'-'+time2 + '秒', location])
    if m6_7:
        for match in m6_7:
            location=match.span(1)
            Cycle=match.group(1)
            time1=match.group(2)
            extra=match.group(3)
            message=''
            if extra=='':
                if Cycle == 'LCL':
                        message = '最小周期为' + time1 + '秒'
                elif Cycle == 'XCL':
                        message = '最大周期为' + time1 + '秒'
                elif Cycle == 'SCL':
                        message = '可变最小周期为' + time1 + '秒'
            else:
                tt=re.search(r'\(was (.{1,8})\)',extra,re.I)
                time2=tt.group(1)
                if Cycle=='LCL':
                    message='最小周期为'+time1+'秒，原定为'+time2+'秒'
                elif Cycle=='XCL':
                    message = '最大周期为' + time1 + '秒，原定为' + time2 + '秒'
                elif Cycle=='SCL':
                    message = '可变最小周期为' + time1 + '秒，原定为' + time2 + '秒'

            m6_resolve.append([message, location])
    if m6_8:
        for match in m6_8:
            location=match.span(1)
            message=match.group(1)
            message1=''
            if message=='MG':
                message1='建立协调关系'
            elif message=='PL' or 'IP':
                message1='绿信比方案建立'
            elif message=='LP':
                message1='协调方案建立'
            elif message=='LP0':
                message1='0号协调方案建立'

            m6_resolve.append([message1, location])
    if m6_9:
        for match in m6_9:
            location=match.span(1)
            time1=match.group(1)
            time2=match.group(3)
            message=''
            if match.group(2)=='':
                message='锁定周期'+time1+'秒(临时)'
            else:
                message='锁定周期'+time1+'秒('+time2+')'

            m6_resolve.append([message, location])
    # if m6_10:
    #     for match in m6_10:
    #         location=match.span(1)
    #         Cycle = match.group(1)
    #         time1 = match.group(2)
    #         message = ''
    #         if Cycle == 'LCL':
    #             message = '最小周期为' + time1 + '秒'
    #         elif Cycle == 'XCL':
    #             message = '最大周期为' + time1 + '秒'
    #         elif Cycle == 'SCL':
    #             message = '可变最小周期为' + time1 + '秒'

            m6_resolve.append([message, location])


    return m6_resolve

def resolve_M7(m7,m_remove,m_active):
    m7_resolve=[]
    m7_5=re.finditer(r'IP=([0-9]+)!(.{0,15})',m7,re.I)
    m7_4=re.finditer(r'([a-z]{1,8}) ALARM (.*)',m7,re.I)
    m7_1=re.finditer(r'([a-z]{1})([a-z]{2});(.{1,15})(!|\()(.*)',m7,re.I)
    #m7_2=re.finditer(r'([a-z]+)/',m7,re.I)
    #m7_3 = re.finditer(r'([a-z]+)=([0-9]+)#;(.{1,15})!', m7, re.I)
    m7_3_1 = re.finditer(r'([a-z]+.)=([0-9]+)#(|;(.{1,15}))!(.*)', m7, re.I)
    m7_3_2 = re.finditer(r'([a-z]+)/', m7, re.I)
    m7_3_3 = re.finditer(r'PL=([0-9]+)(.*)', m7, re.I)
    m7_3_4 = re.finditer(r'PP([0-9]+)(.*)', m7, re.I)
    m7_3_5 = re.finditer(r'([a-z]+)=([0-9]+)/(.*)',m7,re.I)
    m7_6=re.finditer(r'XSF=(\+|-|)([0-9]+)(!|/|#;.{0,15}!)(.{0,15})',m7,re.I)
    m7_7 = re.finditer(r'XSF=(\+|-|)([0-9]+)-([0-9]+)(!|/|#;.{0,15}!)(.{0,15})', m7, re.I)
    # m3_3 = re.search(r'PL=([0-9]+)(.*)', m3, re.I)
    if m7_7:
        for match in m7_7:
            location=match.group(1)
            way=match.group(1)
            number=match.group(2)
            number2=match.group(3)
            time=match.group(4)
            extra=match.group(5)
            if '+' in way:
                way='开启'
            else:
                way='关闭'

            if 'Activated' in extra:
                extra=',该操作生效'
            elif 'Timed' in extra:
                extra=',系统到期，自动解除'
            else:
                extra=''

            if time=='!':
                m7_resolve.append([way+number+'号标志位到'+number2+'号标志位'+extra, location])
            elif time=='/':
                m7_resolve.append([way+number+'号标志位到'+number2+'号标志位,标志位解除'+extra, location])
            else:
                if '*' in time:
                    m7_resolve.append([way + number + '号标志位到'+number2+'号标志位,时长无限' + extra, location])
                else:
                    time_interval=''
                    tt = re.finditer(r'#;([0-9]*)(H|M)([0-9]*)(.?)', time, re.I)
                    if tt:
                        for match in tt:
                            if match.group(2) == 'M' or match.group(2) == 'm':
                                h = str(int(int(match.group(1)) / 60))
                                m = str(int(int(match.group(1)) % 60))
                                time_interval = h + ':' + m + ':00'
                            elif match.group(2) == 'h' or match.group(2) == 'H':
                                if match.group(4) == 'M' or match.group(4) == 'm':
                                    h = str(int(int(match.group(1))) + int(int(match.group(3)) / 60))
                                    m = str(int(int(match.group(3)) % 60))
                                    time_interval = h + ':' + m + ':00'
                                else:
                                    time_interval = match.group(1) + ':00:00'
                    m7_resolve.append([way + number + '号标志位到'+number2+'号标志位,时长'+time_interval + extra, location])
    if m7_6:
        for match in m7_6:
            location=match.span(1)
            way=match.group(1)
            number=match.group(2)
            time=match.group(3)
            extra=match.group(4)
            if '+' in way:
                way='开启'
            else:
                way='关闭'

            if 'Activated' in extra:
                extra=',该操作生效'
            elif 'Timed' in extra:
                extra=',系统到期，自动解除'
            else:
                extra=''

            if time=='!':
                m7_resolve.append([way+number+'号标志位'+extra, location])
            elif time=='/':
                m7_resolve.append([way+number+'号标志位,标志位解除'+extra, location])
            else:
                if '*' in time:
                    m7_resolve.append([way + number + '号标志位,时长无限' + extra, location])
                else:
                    tt=re.search(r'#;(.{1,15})!',time,re.I)
                    time1=tt.group(1)
                    m7_resolve.append([way + number + '号标志位,时长'+time1 + extra, location])
    if m7_5:
        for match in m7_5:
            location=match.span(1)
            number=match.group(1)
            extra=match.group(2)
            if 'Activated' in extra:
                extra=',该操作生效'
            elif 'Timed' in extra:
                extra=',系统到期，自动解除'
            elif extra=='':
                extra=''
            message=number+'号绿信比方案'+extra
            m7_resolve.append([message, location])
    if m7_4:
        for match in m7_4:
            location = match.span(1)
            message='字母乱码，无需翻译'
            m7_resolve.append([message, location])
    if m7_3_5:
        for match in m7_3_5:
            location=match.span(1)
            if 'Activated' in match.group(3):
                ex='该操作生效'
            elif 'removed' in match.group(3):
                ex='系统到期，自动解除'
            else:
                ex=''
            key_word=match.group(1)
            key_word_mean=OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean=key_word
            way=match.group(2)
            m7_resolve.append([key_word_mean+'为'+way+'解除锁定,'+ex,location])
    if m7_3_1:
        for match in m7_3_1:
            location = match.span(1)
            key_word = match.group(1)
            plan_no = match.group(2)
            interval = match.group(3)
            extra=match.group(5)
            mextra=''
            if 'Activated' in extra:
                mextra=',该操作生效'
            else:
                mextra=''
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            if len(interval) > 0:
                time_interval = match.group(4)
                if '*' in time_interval:
                    time_desc = '，时长无限'
                else:
                    time_desc = '，时长' + time_interval
            else:
                time_desc = '；'
            meaning = plan_no + '号' + key_word_mean + '锁定' + time_desc+mextra
            m7_resolve.append([meaning, location])
    if m7_3_2:
        for match in m7_3_2:
            location = match.span(1)
            key_word = match.group(1)
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            meaning = key_word_mean + '解除锁定'
            m7_resolve.append([meaning, location])
    if m7_3_3:
        for match in m7_3_3:
            meaning=''
            location = match.span(1)
            # print(location)
            pl_no = match.group(1)
            meaning_start = pl_no + '号绿信比方案'
            way = match.group(1)
            m3_3_1 = re.search(r' (X|)SF=(.*)', match.group(2), re.I)
            m3_3_2 = re.search(r'([a-z])=.PD([a-z])!', match.group(2), re.I)
            m3_3_3 = re.finditer(r'([a-z])=([0-9]+)(.{0,4})([a-z])!( \(was ([0-9]+)([a-z]{4})([a-z])\)|)', match.group(2), re.I)
            if m3_3_1:
                keyword = m3_3_1.group(1)
                symbol = m3_3_1.group(2)
                meaning = '???'
                if keyword == '':
                    if symbol == '!':
                        meaning = way + '号信绿比方案,' + 'SF为空，非双周期；'
                    else:
                        meaning = way + '号信绿比方案,' + 'SF为' + symbol + '；'
                elif keyword in ('X', 'x'):
                    m3_3_1_1 = re.search(r'([+\-])?([0-9]+)(!|#!|/)', symbol,re.I)
                    if m3_3_1_1:
                        signal = m3_3_1_1.group(1)
                        lamp_no = m3_3_1_1.group(2)
                        symbol1 = m3_3_1_1.group(3)
                        if signal == '+':
                            meaning = way + '号信绿比方案,' + '开启' + lamp_no + '号标志位；'
                        elif signal == '-':
                            meaning = way + '号信绿比方案,' + '关闭' + lamp_no + '号标志位；'
                        elif symbol1 == '/':
                            meaning = way + '号信绿比方案,' + '解锁' + lamp_no + '号标志位；'
                else:
                    meaning = way + '号信绿比方案,' + '???'
            m7_resolve.append([meaning, location])
            if m3_3_2:
                phase = m3_3_2.group(1)
                location=m3_3_2.span(1)
                next_phase = m3_3_2.group(2)
                meaning = way + '号信绿比方案,' + '设定' + phase + '相位为主相位，下一相位为' + next_phase + '；'
                m7_resolve.append([meaning, location])
            if m3_3_3:
                for match in m3_3_3:
                    location = match.span(1)
                    phase = match.group(1)
                    phase_length = match.group(2)
                    phase_func = match.group(3)
                    next_phase = match.group(4)
                    extra=match.group(5)
                    meaning=''
                    if extra=='':
                        if 'NG' in phase_func:
                            func1 = '相位不能提前结束；相位不可以被跳过；'
                        else:
                            func1 = ''
                        meaning = way + '号信绿比方案,' + '设定' + phase + '相位绿信比为' + phase_length + ',' + func1 + '下一相位为' + next_phase+'；'
                    else:
                        m3_3_3_1=re.finditer(r'([0-9]+)([a-z]{4})([a-z])',extra,re.I)
                        for match in m3_3_3_1:
                            length2=match.group(1)
                            next_phase2=match.group(3)
                            meaning=way + '号信绿比方案,' + '设定' + phase + '相位绿信比为' + phase_length + ',不能提前结束，不能被跳过，下一相位为' + next_phase+'，原定为：设定'+phase+'相位绿信比为'+length2+',不能提前结束，不能被跳过，下一相位为'+next_phase2
                    m7_resolve.append([meaning, location])
    if m7_3_4:
        for match in m7_3_4:
            location = match.span(1)
            number=match.group(1)
            way = match.group(2)
            m7_3_4_1=re.finditer(r'([0-9]+),(\+|-|)([0-9]+)\^([a-z]+) \(was ([0-9]+),(\+|-|)([0-9]+)\^([a-z]+)\)',match.group(2),re.I)
            if m7_3_4_1:
                for match in m7_3_4_1:
                    flag=match.group(2)
                    if flag=='-':
                        phase1=match.group(4)
                        phase2=match.group(8)
                        message='内部协调方案锁定为'+number+',比'+phase1+'相位早'+match.group(3)+',原定为比'+phase2+'相位早'+match.group(3)
                        m7_resolve.append([message, location])
                    else:
                        phase1 = match.group(4)
                        phase2 = match.group(8)
                        message = '内部协调方案锁定为' + number + ',比' + phase1 + '相位早' + match.group(
                            3) + ',原定为比' + phase2 + '相位早' + match.group(3)
                        m7_resolve.append([message, location])

            if '#!' in match.group(2):
                m7_resolve.append(['内部协调方案锁定为'+number, location])
            if '#;' in match.group(2):
                m3_4_1 = re.match(r'#;([0-9]*)([a-z]*)([0-9]*)([a-z]*)', match.group(2), re.I)
                m3_4_1_1 = re.finditer(r'#;(.{0,15})!', match.group(2), re.I)
                if m3_4_1:
                    if 'H' in m3_4_1.group(2):
                        time1 = m3_4_1.group(1)
                        if 'M' in m3_4_1.group(4):
                            time2 = m3_4_1.group(3)
                            m7_resolve.append(['内部方案锁定为'+number+'，时长' + time1 + '小时' + time2 + '分钟', location])
                        else:
                            m7_resolve.append(['内部方案锁定为'+number+'，时长' + time1 + '小时', location])
                    if 'M' in m3_4_1.group(2):
                        time1 = m3_4_1.group(1)
                        m7_resolve.append(['内部方案锁定为'+number+'，时长' + time1 + '分钟', location])
                if m3_4_1_1:
                    for match in m3_4_1_1:
                        time1111=match.group(1)
                        if '*' in time1111:
                            m7_resolve.append(['内部方案锁定为' + number + '，时长无限', location])
                        else:
                            m7_resolve.append(['内部方案锁定为' + number + '，时长为'+match.group(1), location])
    if m7_1:
        for match in m7_1:
            phase=match.group(1)
            key_word=match.group(2)
            location=match.span(1)
            time=match.group(3)
            end=match.group(4)
            key_word_mean=OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean=key_word
            if '*' in match.group(3):
                if 'Activated' in match.group(5):
                    m7_resolve.append([phase+'相位'+key_word_mean+'永久锁定,该操作生效',location])
                else:
                    m7_resolve.append([phase + '相位' + key_word_mean + '永久锁定', location])
            else:
                if end=='!':
                    m7_resolve.append([phase+'相位'+key_word_mean+time,location])
                if 'Activated' in match.group(5):
                    m7_resolve.append([phase + '相位' + key_word_mean + time+',操作生效', location])
                if 'no skip' in match.group(5):
                    m7_resolve.append([phase + '相位' + key_word_mean + time + ',不能被跳过', location])
    # if m7_2:
    #     for match in m7_2:
    #         key_word=match.group(1)
    #         location=match.span(1)
    #         key_word_mean=OperateSample.KeyWord.get(key_word.upper())
    #         if not key_word_mean:
    #             key_word_mean=key_word
    #         m7_resolve.append([key_word_mean+'解除',location])
    # if m7_3:
    #     for match in m7_3:
    #         location=match.span(1)
    #         key_word=match.group(1)
    #         number=match.group(2)
    #         way=match.group(3)
    #         key_word_mean=OperateSample.KeyWord.get(key_word.upper())
    #         if '*' in way:
    #             m7_resolve.append([number+'号'+key_word_mean + '永久锁定', location])
    #         else:
    #             m7_resolve.append([number + '号' + key_word_mean + '锁定,时长为：'+way, location])
    return m7_resolve

def resolve_M8(m8,m_remove,m_active):
    m8_resolve=[]
    m8_1=re.finditer(r'(.*)',m8,re.I)
    if m8_1:
        for match in m8_1:
            #userid=match.group(2)
            location=match.span(1)
            message=match.group(1)
            if 'Logoff' in message:
                m8_resolve.append(['用户登出', location])
            if 'Level' in message:
                m8_resolve.append([message+'登入', location])
    return m8_resolve

def resolve_M9(m9,m_remove,m_active):
    m9_resolve=[]
    m9_1=re.finditer(r'D#=(.{1,20})! \(was (.{1,20})\)',m9,re.I)
    m9_2=re.finditer(r'VF=(.{1,20})! \(was (.{1,20})\)',m9,re.I)
    m9_3=re.finditer(r'SD=([0-9]+)([a-z]+)([0-9]+)! \(was (.{1,20})\)',m9,re.I)
    if m9_1:
        for match in m9_1:
            location=match.span(1)
            m9_resolve.append(['线圈编号为'+match.group(1)+'原定为:'+match.group(2), location])
    if m9_2:
        for match in m9_2:
            location=match.span(1)
            m9_resolve.append(['投票流量为'+match.group(1)+'原定为:'+match.group(2), location])
    if m9_3:
        for match in m9_3:
            location = match.span(1)
            m9_resolve.append(['控制策略为' + match.group(1)+'号路口'+match.group(2)+'相位,' + '原定为:' + match.group(4), location])
    return m9_resolve

def resolve_M10(m10,m_remove,m_active):
    m10_resolve=[]
    m10_1 = re.finditer(r'D#=(.{1,20})! \(was (.{1,20})\)', m10, re.I)
    m10_2 = re.finditer(r'VF=(.{1,20})! \(was (.{1,20})\)', m10, re.I)
    m10_3 = re.finditer(r'SD=([0-9]+)([a-z]+)([0-9]+)! \(was (.{1,20})\)', m10, re.I)
    m10_4 = re.finditer(r'([0-9]+)! \(was (.{1,20})\)',m10,re.I)
    m10_5 = re.finditer(r'([0-9]+)([a-z]+)! \(was (.{1,20})\)', m10, re.I)
    if m10_1:
        for match in m10_1:
            location = match.span(1)
            m10_resolve.append(['线圈编号为' + match.group(1) + '原定为:' + match.group(2), location])
    if m10_2:
        for match in m10_2:
            location = match.span(1)
            m10_resolve.append(['投票流量为' + match.group(1) + '原定为:' + match.group(2), location])
    if m10_3:
        for match in m10_3:
            location = match.span(1)
            m10_resolve.append(['控制策略为' + match.group(1) + '号路口' + match.group(2) + '相位,' + '原定为:' + match.group(4), location])
    if m10_4:
        for match in m10_4:
            location=match.span(1)
            m10_resolve.append([match.group(1)+'号子系统，原定：'+match.group(2),location])
    if m10_5:
        for match in m10_5:
            location=match.span(1)
            m10_resolve.append([match.group(1) + '号路口' + match.group(2) + '相位,' + '原定为:' + match.group(3),location])

    return m10_resolve

def resolve_M11(m11,m_remove,m_active):
    m11_resolve = []
    m11_17 = re.finditer(
        r'LP([0-9]+)=(\+|-|)([0-9]+),(\+|-|)([0-9]+)\^([a-z]+)([0-9]+)! \(was (\+|-|)([0-9]+),(\+|-|)([0-9]+)\^([a-z]+)([0-9]+)\)',
        m11, re.I)
    m11_16=re.finditer(r'PP([0-9]+)=(.{0,8})\^([a-z]+)\(was (.{0,8})\^([a-z]+)\)',m11,re.I)
    m11_15 = re.finditer(r'PP([0-9]+)(#!|#;(.{0,15})!)',m11,re.I)
    m11_14 = re.finditer(r'LP0=([0-9])!(.*)', m11, re.I)
    m11_13=re.finditer(r'LP0/(.*)',m11,re.I)
    m11_12=re.finditer(r'([a-z]+)=!(.{0,15})',m11,re.I)
    m11_11=re.finditer(r'(LP0)=([0-9]+)#;(.{1,15})!(.*)',m11,re.I)
    m11_9 = re.finditer(r'CL=([0-9]+)\^(;|)(.{0,15})!', m11, re.I)
    m11_8 = re.finditer(r'([a-z]{2,3})!', m11, re.I)
    m11_7 = re.finditer(r'(LCL|SCL|XCL)=(.{1,15})! \(was (.{1,15})\)', m11, re.I)
    m11_10 = re.finditer(r'(LCL|SCL|XCL)=(.{1,15})!', m11, re.I)
    m11_4 = re.search(r'HCL=([0-9]+)!(.{1,6})([0-9]+)(.)', m11, re.I)
    # m6_3 = re.search(r'lp0=(.{1,5}),(.)([0-9]+)\^([a-z]+)([0-9]+)#!(.*)', m6, re.I)
    m11_1 = re.finditer(r'([a-z]+)(/(.*)|=([0-9]+)#!|=([0-9]{0,5})#;(.{1,10})!(.*))', m11, re.I)
    m11_2 = re.finditer(r'([a-z]+)(#!|#;)(.*)', m11, re.I)
    m11_6 = re.finditer(r'(lp|LP0)=(|-)([0-9]+),(|-)([0-9]+)\^([a-z]{1})([0-9]+)([a-z]{0,1})(#!|#;|#;(.{1,15})!)', m11,re.I)
    m11_18 = re.finditer(r'(lp|LP0)=([0-9]+),(\+|-|)([0-9]+)\^([a-z]+)([0-9]+)(#!|#;!|#;(.{1,15})!|!)', m11,re.I)
    # m11_19 = re.finditer(r'([a-z]+)(/)', m11, re.I)
    m11_20=re.finditer(r'([a-z]+)=([0-9]+)!(.*)',m11,re.I)
    m11_21=re.finditer(r'([a-z]+)([0-9]+)(#!|#;(.{1,10})!)(.*)',m11,re.I)
    if m11_21:
        for match in m11_21:
            location=match.span(1)
            key_word=match.group(1)
            number=match.group(2)
            time=match.group(3)
            extra=match.group(4)
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            if 'Activated' in extra:
                extra=',该操作生效'
            elif 'Timed' in extra:
                extra=',系统到期，自动解除'
            else:
                extra=''
            if time=='#!':
                message=key_word_mean+'方案为'+number+'锁定'+extra
                m11_resolve.append([message, location])
            else:
                if '*' in time:
                    message = key_word_mean + '方案为' + number + '永久锁定' + extra
                    m11_resolve.append([message, location])
                else:
                    tt=re.finditer(r'#;(.{1,10})!',time,re.I)
                    for match in tt:
                        tttt=match.group(1)
                        ttt = re.finditer(r'([0-9]*)(h|m)([0-9]*)(.?)', match.group(1), re.I)
                        if ttt:
                            for match in ttt:
                                if match.group(2) == 'M' or match.group(2) == 'm':
                                    h = str(int(int(match.group(1)) / 60))
                                    m = str(int(int(match.group(1)) % 60))
                                    tttt = h + ':' + m + ':00'
                                elif match.group(2) == 'h' or match.group(2) == 'H':
                                    if match.group(4) == 'M' or match.group(4) == 'm':
                                        h = str(int(int(match.group(1))) + int(int(match.group(3)) / 60))
                                        m = str(int(int(match.group(3)) % 60))
                                        tttt = h + ':' + m + ':00'
                                    else:
                                        tttt = match.group(1) + ':00:00'
                        message=message = key_word_mean + '方案为' + number + '锁定,时长'+tttt + extra
                        m11_resolve.append([message, location])
    if m11_20:
        for match in m11_20:
            location=match.span(1)
            key_word = match.group(1)
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            extra=match.group(3)
            if 'Activated' in extra:
                extra=',该操作生效'
            elif 'Timed' in extra:
                extra=',系统到期，自动解除'
            else:
                extra=''
            message=key_word_mean+'为'+match.group(2)+extra
            m11_resolve.append([message, location])
    # if m11_19:
    #     for match in m11_19:
    #         location=match.span(1)
    #         key_word=match.group(1)
    #         key_word_mean = OperateSample.KeyWord.get(key_word.upper())
    #         if not key_word_mean:
    #             key_word_mean=key_word
    #         message='解除'
    #         m11_resolve.append([key_word_mean+message, location])
    if m11_18:
        for match in m11_18:
            location=match.span(1)
            key=match.group(1)
            if key=='LP0':
                key_word_mean='0号协调方案'
            else:
                key_word_mean=match.group(2)+'号协调方案'
            flag=match.group(3)
            port=match.group(6)
            phase=match.group(5)
            time1=match.group(4)
            tt=match.group(7)
            if flag=='-':
                m11_18_1=re.finditer(r'#;(.{1,15})!',tt,re.I)
                m11_18_2=re.finditer(r'(#!|#;!|!)',tt,re.I)
                if m11_18_1:
                    for match in m11_18_1:
                        ttt=match.group(1)
                        if ttt=='*':
                            message=key_word_mean+'永久锁定,'+'比'+port+'号路口'+phase+'相位早起点'+time1+'秒'
                        else:
                            message = key_word_mean + '比' + port + '号路口' + phase + '相位早起点' + time1 + '秒,时长'+ttt
                        m11_resolve.append([message, location])
                if m11_18_2:
                    for match in m11_18_2:
                        message=key_word_mean + '比' + port + '号路口' + phase + '相位早起点' + time1 + '秒'
                        m11_resolve.append([message, location])
            else:
                m11_18_1 = re.finditer(r'#;(.{1,15})!', tt, re.I)
                m11_18_2 = re.finditer(r'(#!|#;!|!)', tt, re.I)
                if m11_18_1:
                    for match in m11_18_1:
                        ttt = match.group(1)
                        if ttt == '*':
                            message = key_word_mean + '永久锁定,' + '比' + port + '号路口' + phase + '相位晚起点' + time1 + '秒'
                        else:
                            message = key_word_mean + '比' + port + '号路口' + phase + '相位晚起点' + time1 + '秒,时长' + ttt
                        m11_resolve.append([message, location])
                if m11_18_2:
                    for match in m11_18_2:
                        message=key_word_mean + '比' + port + '号路口' + phase + '相位晚起点' + time1 + '秒'
                        m11_resolve.append([message, location])
    if m11_17:
        for match in m11_17:
            location=match.span(1)
            number=match.group(1)
            flag=match.group(2)
            time1=match.group(3)
            time2=match.group(5)
            phase1=match.group(6)
            number1=match.group(7)
            time3 = match.group(9)
            time4=match.group(11)
            phase2 = match.group(12)
            number2 = match.group(13)
            if flag=='-':
                message=number+'号协调方案，比'+number1+'号路口'+phase1+'相位早起点'+time1+'秒到'+time2+'秒，原定为比'+number2+'号路口'+phase2+'相位早起点'+time3+'秒到'+time4+'秒'
                m11_resolve.append([message, location])
            else:
                message = number + '号协调方案，比' + number1 + '号路口' + phase1 + '相位晚起点' + time1 + '秒到' + time2 + '秒，原定为比' + number2 + '号路口' + phase2 + '相位晚起点' + time3 + '秒到' + time4+'秒'
                m11_resolve.append([message, location])
    if m11_16:
        for match in m11_16:
            location=match.span(1)
            number=match.group(1)
            phase1=match.group(3)
            phase2=match.group(5)
            message=number+'号内部方案,相位为'+phase1+',原定为'+phase2+'相位'
            m11_resolve.append([message, location])
    if m11_15:
        for match in m11_15:
            location=match.span(1)
            number=match.group(1)
            if match.group(2)=='#!':
                message=number+'号内部方案锁定'
                m11_resolve.append([message, location])
            else:
                m11_15_1=re.finditer(r'#;(.{0,15})!',match.group(2),re.I)
                for match in m11_15_1:
                    time=match.group(1)
                    message=number+'号内部方案锁定,时长'+time
                    m11_resolve.append([message, location])
    if m11_14:
        for match in m11_14:
            location=match.span(1)
            extra=match.group(2)
            if 'Activated' in extra:
                extra=',该操作生效'
            elif 'Timed' in extra:
                extra=',系统到期，自动解除'
            else:
                extra=''
            message='0号协调方案'+extra
            m11_resolve.append([message, location])
    if m11_13:
        for match in m11_13:
            location = match.span(1)
            extra=match.group(1)
            if 'Activated' in extra:
                extra=',该操作生效'
            elif 'Timed' in extra:
                extra=',系统到期，自动解除'
            else:
                extra=''
            message='解除0号协调方案'+extra
            m11_resolve.append([message, location])
    if m11_12:
        for match in m11_12:
            location=match.span(1)
            key_word=match.group(1)
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            extra=match.group(2)
            if 'Activated' in extra:
                extra='，该操作生效'
            elif 'Timed' in extra:
                extra=',系统到期，自动生效'
            else:
                extra=''
            message=key_word_mean+'现在意义不明'+extra
            m11_resolve.append([message, location])
    if m11_11:
        for match in m11_11:
            location=match.span(1)
            key_word=match.group(1)
            way=match.group(2)
            time=match.group(3)
            ex=match.group(4)
            if 'Activated' in ex:
                ex=',该操作生效'
            elif 'remove' in ex:
                ex=',系统到期，自动解除'
            else:
                ex=''
            if '*' in time:
                time='无限'
            else:
                time=time
            if key_word=='LP':
                key_word_mean=OperateSample.KeyWord.get(key_word.upper())
                if not key_word_mean:
                    key_word_mean=key_word
                m11_resolve.append([key_word_mean+'方案为'+way+',时长'+time+ex,location])
            else:
                key_word_mean = OperateSample.KeyWord.get(key_word.upper())
                if not key_word_mean:
                    key_word_mean=key_word
                m11_resolve.append([key_word_mean + ',时长' + time + ex, location])
    if m11_1:
        for match in m11_1:
            location = match.span(1)
            key_word = match.group(1)
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            flag = match.group(2)
            if '/' in flag:
                ex=re.finditer(r'/(.*)',flag,re.I)
                extra=''
                for match in ex:
                    extra=match.group(1)
                    if 'Activated' in extra:
                        extra=',该操作生效'
                    elif 'Timed' in extra:
                        extra=',系统到期，自动解除'
                    else:
                        extra=''
                m11_resolve.append([key_word_mean + '锁定解除'+extra, location])
            if '#!' in flag:
                m6_1_1 = re.search(r'=([0-9]+)#!', flag, re.I)
                time1 = m6_1_1.group(1)
                m11_resolve.append([key_word_mean + '锁定为' + time1, location])
            if '#;' in flag:
                m11_1_1=re.finditer(r'([0-9]*)#;(.{0,15})!(.*)',flag,re.I)
                for match in m11_1_1:
                    time=match.group(2)
                    tt=match.group(1)
                    if 'Activated' in match.group(3):
                        if '*' in match.group(2):
                            m11_resolve.append([key_word_mean+'锁定'+tt+',时长无限,该操作生效',location])
                        else:
                            ttt = re.finditer(r'([0-9]*)(h|m)([0-9]*)(.?)',match.group(2) , re.I)
                            if ttt:
                                for match in ttt:
                                    if match.group(2) == 'M' or match.group(2) == 'm':
                                        h = str(int(int(match.group(1)) / 60))
                                        m = str(int(int(match.group(1)) % 60))
                                        time = h + ':' + m + ':00'
                                    elif match.group(2) == 'h' or match.group(2) == 'H':
                                        if match.group(4) == 'M' or match.group(4) == 'm':
                                            h = str(int(int(match.group(1))) + int(int(match.group(3)) / 60))
                                            m = str(int(int(match.group(3)) % 60))
                                            time = h + ':' + m + ':00'
                                        else:
                                            time = match.group(1) + ':00:00'
                            m11_resolve.append([key_word_mean + '锁定'+tt+'，时长'+time+'该操作生效', location])
                    else:
                        if '*' in match.group(2):
                            m11_resolve.append([key_word_mean+'锁定'+tt+'，时长无限',location])
                        else:
                            ttt = re.finditer(r'([0-9]*)(h|m)([0-9]*)(.?)', match.group(2), re.I)
                            if ttt:
                                for match in ttt:
                                    if match.group(2) == 'M' or match.group(2) == 'm':
                                        h = str(int(int(match.group(1)) / 60))
                                        m = str(int(int(match.group(1)) % 60))
                                        time = h + ':' + m + ':00'
                                    elif match.group(2) == 'h' or match.group(2) == 'H':
                                        if match.group(4) == 'M' or match.group(4) == 'm':
                                            h = str(int(int(match.group(1))) + int(int(match.group(3)) / 60))
                                            m = str(int(int(match.group(3)) % 60))
                                            time = h + ':' + m + ':00'
                                        else:
                                            time = match.group(1) + ':00:00'
                            m11_resolve.append([key_word_mean + '锁定' + tt + '，时长' + time, location])
    if m11_2:
        for match in m11_2:
            location = match.span(1)
            key_word = match.group(1)
            key_word_mean = OperateSample.KeyWord.get(key_word.upper())
            if not key_word_mean:
                key_word_mean = key_word
            way = match.group(2)
            if '/' in match.group(2):
                m11_resolve.append([key_word_mean + '锁定解除', location])
            if '#!' in match.group(2):
                m11_resolve.append([key_word_mean + '锁定', location])
            if '#;' in match.group(2):
                if '*' in match.group(3):
                    m11_resolve.append([key_word_mean + '锁定，时长无限', location])
                else:
                    ttt = re.finditer(r'([0-9]*)(h|m)([0-9]*)(.?)', match.group(3), re.I)
                    time=''
                    if ttt:
                        for match in ttt:
                            time = ''
                            if match.group(2) == 'M' or match.group(2) == 'm':
                                h = str(int(int(match.group(1)) / 60))
                                m = str(int(int(match.group(1)) % 60))
                                time = h + ':' + m + ':00'
                            elif match.group(2) == 'h' or match.group(2) == 'H':
                                if match.group(4) == 'M' or match.group(4) == 'm':
                                    h = str(int(int(match.group(1))) + int(int(match.group(3)) / 60))
                                    m = str(int(int(match.group(3)) % 60))
                                    time = h + ':' + m + ':00'
                                else:
                                    time = match.group(1) + ':00:00'
                    m11_resolve.append([key_word_mean + '锁定，时长为' + time, location])
    # if m6_3:
    #     location=m6_3.span(1)
    #     port=m6_3.group(5)
    #     phase=m6_3.group(4)
    #     time1=m6_3.group(3)
    #     flag=m6_3.group(2)
    #     if flag=='+':
    #         m6_resolve.append(['锁定0号协调方案，方案内容：比'+ port +'号路口'+phase+'相位晚起点'+time1+'秒。',location])
    #     else:
    #         m6_resolve.append(['锁定0号协调方案，方案内容：比' + port + '号路口' + phase + '相位早起点' + time1 + '秒。', location])
    if m11_4:
        location = m11_4.span(1)
        time1 = m11_4.group(1)
        time2 = m11_4.group(3)
        m11_resolve.append([',原定周期为' + time2 + ',最大周期为' + time1 + ';', location])
    if m11_6:
        for match in m11_6:
            location = match.span(1)
            port = match.group(7)
            phase = match.group(6)
            flag = match.group(2)
            time1 = match.group(3)
            time2 = match.group(5)
            long = match.group(9)
            if long == '#!':
                if flag == '+' or '':
                    m11_resolve.append(
                        ['锁定0号协调方案，方案内容：比' + port + '号路口' + phase + '相位晚起点' + time1 + '-' + time2 + '秒',
                         location])
                if flag == '-':
                    m11_resolve.append(
                        ['永久锁定0号协调方案，方案内容：比' + port + '号路口' + phase + '相位早起点' + time1 + '-' + time2 + '秒',
                         location])
            elif long == '#;':
                if flag == '+' or '':
                    m11_resolve.append(['锁定0号协调方案，方案内容：比' + port + '号路口' + phase + '相位晚起点' + time1 + '-' + time2 + '秒', location])
                if flag == '-':
                    m11_resolve.append(['锁定0号协调方案，方案内容：比' + port + '号路口' + phase + '相位早起点' + time1 + '-' + time2 + '秒', location])
            else:
                time111=''
                m11_6_1=re.finditer(r'#;(.{1,15})!',long,re.I)
                for match in m11_6_1:
                    time111=match.group(1)
                if flag == '+' or '':
                    m11_resolve.append(['锁定0号协调方案，方案内容：比' + port + '号路口' + phase + '相位晚起点' + time1 + '-' + time2 + '秒,时长'+time111, location])
                if flag == '-':
                    m11_resolve.append(['锁定0号协调方案，方案内容：比' + port + '号路口' + phase + '相位早起点' + time1 + '-' + time2 + '秒,时长'+time111, location])
    if m11_7:
        for match in m11_7:
            location = match.span(1)
            Cycle = match.group(1)
            time1 = match.group(2)
            time2 = match.group(3)
            message = ''
            if Cycle == 'LCL':
                message = '最小周期为' + time1 + '秒，原定为' + time2 + '秒'
            elif Cycle == 'XCL':
                message = '最大周期为' + time1 + '秒，原定为' + time2 + '秒'
            elif Cycle == 'SCL':
                message = '可变最小周期为' + time1 + '秒，原定为' + time2 + '秒'

            m11_resolve.append([message, location])
    if m11_8:
        for match in m11_8:
            location = match.span(1)
            message = match.group(1)
            message1 = ''
            if message == 'MG':
                message1 = '建立协调关系'
            elif message == 'PL' or 'IP':
                message1 = '绿信比方案建立'
            elif message == 'LP':
                message1 = '协调方案建立'
            elif message == 'LP0':
                message1 = '0号协调方案建立'

            m11_resolve.append([message1, location])
    if m11_9:
        for match in m11_9:
            location = match.span(1)
            time1 = match.group(1)
            time2 = match.group(3)
            message = ''
            if match.group(2) == '':
                message = '锁定周期' + time1 + '秒(临时)'
            else:
                message = '锁定周期' + time1 + '秒(' + time2 + ')'

            m11_resolve.append([message, location])
    if m11_10:
        for match in m11_10:
            location = match.span(1)
            Cycle = match.group(1)
            time1 = match.group(2)
            message = ''
            if Cycle == 'LCL':
                message = '最小周期为' + time1 + '秒'
            elif Cycle == 'XCL':
                message = '最大周期为' + time1 + '秒'
            elif Cycle == 'SCL':
                message = '可变最小周期为' + time1 + '秒'

            m11_resolve.append([message, location])

    return m11_resolve

def resolve_operate_data(data):
    resolve_data = []
    #record_meaning=''
    if data:
        meaning=''
        record_meaning = ''
        oper_code=str(data[0])
        if oper_code:
            # print(oper_code)
            # re.match匹配开头
            m0 = re.match(r'([0-9]+): ([0-9]+)!(.*)', oper_code, re.I)
            m1 = re.match(r'SS=([0-9]+)(.*)', oper_code, re.I)
            m2 = re.match(r'IP(.*)', oper_code, re.I)
            m3 = re.match(r'([0-9]+): I=([0-9]+)(.*)', oper_code, re.I)
            m4 = re.match(r'PL(.*)', oper_code, re.I)
            m5 = re.match(r'XSF(.*)', oper_code, re.I)
            m6 = re.match(r'([0-9]+): SS=([0-9]+)(.*)', oper_code, re.I)
            m7 = re.match(r'KEY=([0-9]+) I=([0-9]+)(.*)',oper_code,re.I)
            m8 = re.match(r'Term ([0-9]+) \((.{1,30})\) User ([0-9]+)(.*)',oper_code,re.I)
            m9 = re.match(r'KEY=([0-9]+) (SI|SA)=([0-9]+)(.*)',oper_code,re.I)
            m10=re.match(r'KEY=([0-9]+) ([a-z]+)([0-9]{1,5})=(.*)',oper_code,re.I)
            m11=re.match(r'KEY=([0-9]+) SS=([0-9]+)(.*)',oper_code,re.I)
            m_remove = re.search(r'(Timed - removed by system)', oper_code, re.I)
            m_active = re.search(r'(Activated)', oper_code, re.I)
            #m7=re.match(r'PL=0 SF=! (was DC)',oper_code,re.I)

            #if m7:
                #resolve_data.append([oper_code,'信绿比方案设定为0，原定为双周期，实际为空'])

            # print("m0:", m0)
            if m0:
                siteid = m0.group(2)
                action = m0.group(1)
                # old_plan = re.match(r'%s([0-9]+):' % m0.group(2), m0.group(1))
                m0_resolve = []
                # old_plan_no = old_plan.group(1)
                m0_resolve = re_match_M0(m0.group(3), m_remove, m_active)
                sorted_record = resolve_dispose(m0_resolve)
                if sorted_record==[]:
                    aa.append(oper_code)
                record_meaning = action + '号Action,内容：' + siteid + '号路口'
                for record in sorted_record:
                    record_meaning = record_meaning + record
                #resolve_data.append([userid, region, siteid, oper_time,  oper_code, record_meaning])
                resolve_data.append([oper_code,record_meaning])
            if m1:
                ss_no = m1.group(1)
                m1_resolve = re_match_M1(m1.group(2), m_remove, m_active)
                sorted_record = resolve_dispose(m1_resolve)
                #if siteid:
                    #record_meaning = siteid + "号路口" + ss_no + "号子系统"
                #else:
                record_meaning = ss_no + "号子系统"
                for record in sorted_record:
                    record_meaning = record_meaning + record
                if sorted_record==[]:
                    aa.append(oper_code)
                #resolve_data.append([userid, region, siteid, oper_time, oper_code, record_meaning])
                resolve_data.append([oper_code,record_meaning])
            if m2:
                m2_resolve = resolve_M2(m2.group(1), m_remove, m_active)
                sorted_record = resolve_dispose(m2_resolve)
                record_meaning = ''
                for record in sorted_record:
                    record_meaning = record_meaning + record
                if sorted_record==[]:
                    aa.append(oper_code)
                #resolve_data.append([userid, region, siteid, oper_time, oper_code, record_meaning])
                resolve_data.append([oper_code,record_meaning])
            if m3:
                action_no = m3.group(1)
                siteid = m3.group(2)
                m3_resolve = resolve_M3(m3.group(3), m_remove ,m_active)

                sorted_record = resolve_dispose(m3_resolve)
                record_meaning = action_no +'号Action,内容：'+siteid+'号路口，'
                for record in sorted_record:
                    record_meaning = record_meaning + record
                if sorted_record==[]:
                    aa.append(oper_code)
                #resolve_data.append([userid, region, siteid, oper_time, oper_code, record_meaning])
                resolve_data.append([oper_code,record_meaning])
                pass
            if m4:
                action_no=m4.group(1)
                m4_resolve=resolve_M4(m4.group(1), m_remove, m_active)
                sorted_record=resolve_dispose(m4_resolve)
                record_meaning=''
                if sorted_record[0]!=[]:
                   record_meaning=record_meaning + sorted_record[0][0]
                if sorted_record==[]:
                    aa.append(oper_code)
                #resolve_data.append([userid, region, siteid, oper_time, oper_code, record_meaning])
                resolve_data.append([oper_code,record_meaning])
            if m5:
                m5_resolve = resolve_M5(m5.group(1), m_remove, m_active)
                sorted_record = resolve_dispose(m5_resolve)
                record_meaning = ''
                for record in sorted_record:
                    record_meaning = record_meaning + record
                if sorted_record==[]:
                    aa.append(oper_code)
                #resolve_data.append([userid, region, siteid, oper_time, oper_code, record_meaning])
                resolve_data.append([oper_code,record_meaning])
            if m6:
                action_no=m6.group(1)
                siteid=m6.group(2)
                m6_resolve = resolve_M6(m6.group(3), m_remove, m_active)

                sorted_record = resolve_dispose(m6_resolve)
                record_meaning = action_no + '号Action,内容：' + siteid + '号子系统，'
                for record in sorted_record:
                    record_meaning = record_meaning + record
                if sorted_record==[]:
                    aa.append(oper_code)
                #resolve_data.append([userid, region, siteid, oper_time, oper_code, record_meaning])
                resolve_data.append([oper_code,record_meaning])
            if m7:
                user=m7.group(1)
                site=m7.group(2)
                m7_resolve = resolve_M7(m7.group(3),m_remove,m_active)
                # print('aaaa+:',m7_resolve)
                sorted_record = resolve_dispose(m7_resolve)
                record_meaning=user+'号用户,'+site+'号路口,'
                for record in sorted_record:
                    record_meaning = record_meaning+record
                if '字母乱码，无需翻译' in record_meaning:
                    record_meaning=''
                else:
                    record_meaning=record_meaning
                resolve_data.append([oper_code,record_meaning])
            if m8:
                number1=m8.group(1)
                number2=m8.group(3)
                m8_resolve=resolve_M8(m8.group(4),m_remove,m_active)
                sorted_record=resolve_dispose(m8_resolve)
                record_meaning=number1+'号终端,'+number2+'号用户，'
                for record in sorted_record:
                    record_meaning=record_meaning+record
                resolve_data.append([oper_code, record_meaning])
            if m9:
                number1=m9.group(1)
                message=m9.group(2)+m9.group(3)
                m9_resolve = resolve_M9(m9.group(4), m_remove, m_active)
                sorted_record=resolve_dispose(m9_resolve)
                record_meaning=number1+'号用户,'+message+':'
                for record in sorted_record:
                    record_meaning=record_meaning+record
                resolve_data.append([oper_code, record_meaning])
            if m10:
                number1=m10.group(1)
                message=m10.group(2)+m10.group(3)
                m10_resolve = resolve_M10(m10.group(4), m_remove, m_active)
                sorted_record = resolve_dispose(m10_resolve)
                record_meaning = number1 + '号用户,' + message + ':'
                for record in sorted_record:
                    record_meaning = record_meaning + record
                resolve_data.append([oper_code, record_meaning])
            if m11:
                user = m11.group(1)
                site = m11.group(2)
                m11_resolve = resolve_M11(m11.group(3), m_remove, m_active)
                sorted_record = resolve_dispose(m11_resolve)
                record_meaning = user + '号用户,' + site + '号子系统,'
                for record in sorted_record:
                    record_meaning = record_meaning + record
                resolve_data.append([oper_code, record_meaning])


            meaning=record_meaning
        return meaning
    # print(resolve_data)
    df_resolve_data = pd.DataFrame(resolve_data)
    aaa=pd.DataFrame(aa)



def main():
    operate_data = call_oracle_operate_data('null')
    resolve_operate_data(operate_data)


if __name__ == "__main__":
    test = False

    if test:
        oper_code = r'SS=16 LP0=0#;1:00:00! (Activated)'
        m0 = re.match(r'([0-9]+:) ([0-9]+)!(.*)', oper_code, re.I)
        m1 = re.match(r'SS=([0-9]+)(.*)', oper_code, re.I)
        m2 = re.match(r'IP(.*)', oper_code, re.I)
        m3 = re.match(r'([0-9]+:) I=([0-9]+)(.*)', oper_code, re.I)
        m4 = re.match(r'PL(.*)', oper_code, re.I)
        m5 = re.match(r'XSF(.*)', oper_code, re.I)
        m6 = re.match(r'([0-9]+:) SS=([0-9]+)(.*)', oper_code, re.I)
        m_remove = re.search(r'(Timed - removed by system)', oper_code, re.I)
        m_active = re.search(r'(Activated)', oper_code, re.I)
        if m1:
            # print(m1.group(1), m1.group(2))
            result = re_match_M1(m1, m_remove, m_active)
            # print(result)
        if m0:
            siteid = m0.group(2)
            old_plan = re.match(r'%s([0-9]+):' % m0.group(2), m0.group(1))
            m0_resolve = []
            if old_plan:
                old_plan_no = old_plan.group(1)
                m0_resolve = re_match_M0(m0, m_remove, m_active)
                sorted_record = resolve_dispose(m0_resolve)
                record_meaning = siteid + " 路口" + old_plan_no + "号固化方案"
                for record in sorted_record:
                    record_meaning = record_meaning + record

                # with open(r'../EVENT_LOG/record.txt', 'a') as f:
                #     print(record_resolve, file=f)
                #     print(record_resolve)
            else:
                print("bad operate")
    else:
        main()