import psycopg2
# from .database import *
from database import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys

from scipy.stats import norm
from sklearn.neighbors import KernelDensity
import re,os
import datetime as dt
import matplotlib.dates as mdate
import math
from mpl_toolkits.mplot3d import Axes3D
from sklearn.cluster import DBSCAN
#eps为距离阈值ϵ，min_samples为邻域样本数阈值MinPts,X为数据
# 图表显示中文
plt.style.use('ggplot')
plt.rcParams['font.sans-serif']=['SimHei']
plt.rcParams['axes.unicode_minus'] = False

INT_NUM = 15
TIME_POINT = 96
# GROUPCOLUMN = 'date'
GROUPCOLUMN = 'weekday'
DRAW_INTERVAL = 15
PLOT_DATA_SAVE = r"insert into kde_alarm_predict values(%s,%s,%s,%s,%s,%s)"
PLOT_DATA_SAVE2 = r'insert into disposal_alarm_kde_distribution values(%s,%s,%s,%s,%s,%s,%s)'
KDE_RANGE = 1.2
KDE_STATISTIC = r"insert into disposal_alarm_kde_statistic " \
                r"select site_id,dir_desc,weekday,time_point,round(avg(kde_map)::NUMERIC,2) as avg_kde," \
                r"round(stddev_pop(kde_map)::NUMERIC,2)as stddev_kde ,current_date " \
                r"from disposal_alarm_kde_distribution where date >'%s' " \
                r"GROUP BY site_id,dir_desc,weekday,time_point " \
                r"ORDER BY site_id,weekday,dir_desc,time_point"

class DateTimeError(Exception):
    def __init__(self,ErrorInfo):
        super().__init__(self) #初始化父类
        self.errorinfo=ErrorInfo
    def __str__(self):
        return self.errorinfo


class Alarm_match(object):
    def __init__(self, rdsect_id=None):
        self.rdsect_id = rdsect_id
        self.data = self.check_data(DATETIME)
        self.sql_inf()
        self.call_rdsect_inf()

    def check_data(self, date_time):
        print(date_time)
        m = re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2};[0-9]{4}-[0-9]{2}-[0-9]{2};'
                     r'[0-9]{2}:[0-9]{2}:[0-9]{2};[0-9]{2}:[0-9]{2}:[0-9]{2}', date_time)
        if m:
            print(m)
            return DATETIME
        else:
            raise DateTimeError('日期格式错误')

    def sql_inf(self):
        print(self.data.split(';'))
        sdate,edate,stime,etime = self.data.split(';')
        # 获取历史报警数据
        self.sql1 = "select scats_id,inter_name,date_day,time_point,vehicle_dir,delay_value from alarm_temp where date_day " \
              " ='{0}'  and time_point between '{1}' and '{2}'order by date_day,time_point,scats_id" \
            .format(sdate, stime, etime)
        # 获取关联报警数据
        self.sql2 = "select rdsectid,up_to_down_match_num,time_interval,length,score from alarm_rdsect_match_result where date_day = '{0}' and left(time_interval,7)" \
               " between '{1}' and '{2}' ".format(sdate, stime, etime)
        # 根据时间获取关联报警数据
        self.sql4 = "SELECT a.rdsectid, a.up_to_down_match_num,a.time_interval,a.LENGTH,a.score,b.up_node,b.down_node, b.import_desc, b.export_desc" \
                       " FROM alarm_rdsect_match_result a, alarm_rdsect b WHERE a.rdsectid = b.rdsectid and  " \
                       "a.date_day = '{0}' AND LEFT (a.time_interval, 8) BETWEEN '{1}' AND '{2}' AND " \
                       "a.rdsectid IN ({3})".format(sdate, stime, etime, self.rdsect_id)
        # 获取道路基础信息
        self.sql5 = "select rdsectid,up_node,down_node,import_desc,export_desc,fstr_desc from alarm_rdsect "

        # 匹配报警记录与路段ID
        self.sql6 = "select a.*,b.rdsectid ,(case when (a.inter_id=b.gaode_intid_down and to_char(a.f_angle,'999.9')" \
                    " ~ b.f_angle) then b.down_node when (a.inter_id=b.gaode_intid_up and to_char(a.t_angle,'999.9')" \
                    " ~ b.t_angle) then b.up_node end) as scats_id,(case when (a.inter_id=b.gaode_intid_down and " \
                    "to_char(a.f_angle,'999.9') ~ b.f_angle) then  b.import_desc when (a.inter_id=b.gaode_intid_up " \
                    "and to_char(a.t_angle,'999.9') ~ b.t_angle) then b.export_desc end) as dir_desc " \
                    "from (select inter_id,inter_name,coors,time_point,t_angle,f_angle,delay " \
                    "from disposal_alarm_data where to_char(time_point,'yyyy-mm-dd') between '{0}' and '{1}' " \
                    "and to_char(time_point,'hh24:mi:ss') between '{2}' and '{3}')a LEFT JOIN " \
                    "gaode_alarm_rdsect_match b " \
                    "on ((a.inter_id=b.gaode_intid_down and b.f_angle!='-1' and to_char(a.f_angle,'999.9') ~ b.f_angle) " \
                    "or (a.inter_id=b.gaode_intid_up and b.t_angle!='-1' and to_char(a.t_angle,'999.9') ~ b.t_angle)) " \
                    "order by a.inter_id,a.time_point;"\
                    .format(sdate,edate,stime,etime)

    def call_pg_data(self, sql):
        def tuple2frame(result, index):
            column_name = []
            for i in range(len(index)):
                index_name = index[i][0]
                column_name.append(index_name)
            print(column_name)
            result = pd.DataFrame(result, columns=column_name)
            return result
        db = Postgres()
        db.db_conn()
        cr = db.cr
        if cr:
            cr.execute(sql)
            index = cr.description
            result = cr.fetchall()
            db.db_close()
            if result:
                fresult = tuple2frame(result, index)
                return fresult
            else:
                return pd.DataFrame({})
        else:
            print("database connect failed")
            sys.exit(0)
            # return pd.DataFrame({})

    def call_rdsect_inf(self):
        rdsect_inf = self.call_pg_data(self.sql5)
        self.match_rdsect_inf = rdsect_inf[rdsect_inf['rdsectid'] == self.rdsect_id]
        return rdsect_inf

    def call_normal_alarm_data(self):
        if self.rdsect_id:
            conn_alarm_data = self.call_pg_data(self.sql4)
            normal_alarm_data = self.call_pg_data(self.sql1)
        else:
            conn_alarm_data = self.call_pg_data(self.sql2)
            normal_alarm_data = self.call_pg_data(self.sql1)
        return normal_alarm_data, conn_alarm_data

    def call_new_alarm_data(self):
        print(self.sql6)
        new_alarm_data = self.call_pg_data(self.sql6)
        return new_alarm_data

    def match_rdsect_alarm_data(self):
        normal_alarm_data, conn_alarm_data = self.call_normal_alarm_data()
        up_node = self.match_rdsect_inf.get('up_node')
        down_node = self.match_rdsect_inf.get('down_node')
        up_dir = self.match_rdsect_inf.get('export_desc')
        down_dir = self.match_rdsect_inf.get('import_desc')
        up_normal_alarm_data = normal_alarm_data[(normal_alarm_data['scats_id'] == up_node) &
                                                 (normal_alarm_data['vehilce_dir'] == up_dir)]
        dowm_normal_alarm_data = normal_alarm_data[(normal_alarm_data['scats_id'] == down_node) &
                                                   (normal_alarm_data['vehilce_dir'] == down_dir)]
        print(up_normal_alarm_data,dowm_normal_alarm_data)


class ShowProcess():
    """
    显示处理进度的类
    调用该类相关函数即可实现处理进度的显示
    """
    i = 0 # 当前的处理进度
    max_steps = 0 # 总共需要处理的次数
    max_arrow = 50 #进度条的长度
    infoDone = 'done'

    # 初始化函数，需要知道总共的处理次数
    def __init__(self, max_steps, infoDone = 'Done'):
        self.max_steps = max_steps
        self.i = 0
        self.infoDone = infoDone

    # 显示函数，根据当前的处理进度i显示进度
    # 效果为[>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>]100.00%
    def show_process(self, i=None):
        if i is not None:
            self.i = i
        else:
            self.i += 1
        num_arrow = int(self.i * self.max_arrow / self.max_steps) #计算显示多少个'>'
        num_line = self.max_arrow - num_arrow #计算显示多少个'-'
        percent = self.i * 100.0 / self.max_steps #计算完成进度，格式为xx.xx%
        process_bar = '[' + '>' * num_arrow + '-' * num_line + ']'\
                      + '%.2f' % percent + '%' + '\r' #带输出的字符串，'\r'表示不换行回到最左边
        print(process_bar)
        # 控制台显示
        sys.stdout.write(process_bar) #这两句打印字符到终端
        sys.stdout.flush()
        if self.i >= self.max_steps:
            self.close()

    def close(self):
        print('')
        print(self.infoDone)
        self.i = 0


class Kernal(object):
    def __init__(self, plot_data, X_plot, X_axis=None, date=None):
        self.data = plot_data
        self.X_plot = X_plot
        self.X_axis = X_axis
        self.date = date
        self.plot_save = None
        # self.draw_plot(plot_data,X_plot, X_axis, date)

    def kernel_predict(self, X):
        X_plot = self.X_plot
        for kernel in ['gaussian',
                       # 'tophat', 'epanechnikov'
                       ]:
            kde = KernelDensity(kernel=kernel, bandwidth=1).fit(X)
            log_dens = kde.score_samples(X_plot)
        return log_dens

    def save_everyday_kde(self):
        plot_data = self.data
        X_plot = self.X_plot
        X_axis = self.X_axis
        int_list = list(plot_data.keys())
        plot_data_save = []
        max_steps = len(int_list)
        process_bar = ShowProcess(max_steps, 'OK')
        if self.plot_save:
            for i in range(len(int_list)):
                process_bar.show_process()
                # ax = plt.subplot(331)
                int_match_data = plot_data[int_list[i]]
                date_list = list(int_match_data.keys())
                time_aixs = np.arange(0, int(24 * 60 / DRAW_INTERVAL), 12)
                # ax.set_xticks(time_aixs)
                time_list = [str(i)[11:13] for i in X_axis[:, 0]]
                for index_date, value in enumerate(date_list):
                    plt_location = 330 + index_date + 1
                    week_match_data = int_match_data[date_list[index_date]]
                    date = value
                    dir_desc = list(week_match_data.keys())
                    for index_dir, value in enumerate(dir_desc):
                        dir = value
                        X = week_match_data[dir][0]
                        days = week_match_data[dir][4]
                        avg_alarm = len(X) / days
                        X_new = week_match_data[dir][2]
                        X_time = week_match_data[dir][1]
                        int_name = week_match_data[dir][3]
                        if len(X) > 0:
                            log_dens_his = self.kernel_predict(X)
                            log_dens_his_data = list(avg_alarm * np.exp(log_dens_his))
                        else:
                            log_dens_his = None
                            log_dens_his_data = None
                        # 计算核密度分布
                        # log_dens_his = self.kernel_predict(X)
                        total_width, n = 0.8, 2
                        width = total_width / n
                        # log_dens_new = self.kernel_predict(X_new)
                        # log_dens_new_data = list(new_alarm * np.exp(log_dens_new))
                        log_dens_new_data = None
                        plot_data_save.append(
                            [int_list[i], int_name, date , dir, list(X_plot[:, 0]), log_dens_his_data,
                             log_dens_new_data])
            return plot_data_save

    def draw_2d_plot(self):
        plot_data = self.data
        X_plot = self.X_plot
        X_axis = self.X_axis
        date = self.date
        ax = plt.subplot(111)
        keys = plot_data.keys()
        keys = list(keys)
        print("keys", keys)

        ax.legend(loc='upper left')
        # ax.plot(X[:, 0], -0.005 - 0.01 * np.random.random(X.shape[0]), '+k')
        # ax.plot(X_time[:, 0], -0.005 - 0.01 * np.random.random(X.shape[0]), '+k')
        # ax.set_zlim(0, 1000)
        for i in range(len(keys)):
            name = keys[i]
            X_time = plot_data[name][1]
            # Z = np.array([i for m in range(len(X_plot))])
            X = plot_data[name][0]
            print(X)
            # ax.fill(X_plot[:, 0], true_dens, fc='black', alpha=0.2,
            #         label='input distribution')
            for kernel in ['gaussian',
                           # 'tophat', 'epanechnikov'
                           ]:
                kde = KernelDensity(kernel=kernel, bandwidth=0.5).fit(X)
                log_dens = kde.score_samples(X_plot)
                print('plot_data:', X_axis[:, 0], np.exp(log_dens))
                ax.plot(X_axis[:, 0], np.exp(log_dens), '-', label="Int = '{0}'".format(name))
            # ax.text(6, 0.38, "N={0} points".format(N))

        datetime_start = dt.datetime.strptime(str(date) + " 00:00:00", '%Y-%m-%d %H:%M:%S')
        datatime_1day = dt.timedelta(days=1)
        datetime_end = datetime_start + datatime_1day
        ax.xaxis.set_major_formatter(mdate.DateFormatter('%H'))
        ax.set_xlim([datetime_start, datetime_end])
        ax.set_xticks(pd.date_range(datetime_start, datetime_end, freq='1H'))
        # ax.set_xlim(0, 95)
        ax.set_ylim(-0.02, 1.5)
        plt.title('各路口报警数据核密度2d分布', fontsize=20)
        # fig.savefig("G:\plot_pic\\" + system_id + '-' + lane_no + '.png', dpi=75)
        # plt.savefig(r'..\alarm_predict\plot_pic\all_result.png', dpi=150)
        plt.show()
        plt.close()

    def draw_3d_plot(self):
        plot_data = self.data
        X_plot = self.X_plot
        X_axis = self.X_axis
        date = self.date
        fig = plt.figure(figsize=(40, 30))
        ax = fig.gca(projection='3d')
        keys = plot_data.keys()
        keys = list(keys)
        print("keys", keys)
        # ax.set_zlim(0, 1000)

        for i in range(len(keys)):
            name = keys[i]
            X_time = plot_data[name][1]
            Y = np.array([i*5 for m in range(len(X_plot))])
            X = plot_data[name][0]
            print(X)
            # ax.fill(X_plot[:, 0], true_dens, fc='black', alpha=0.2,
            #         label='input distribution')
            for kernel in ['gaussian',
                           # 'tophat', 'epanechnikov'
                           ]:
                kde = KernelDensity(kernel=kernel, bandwidth=0.5).fit(X)
                log_dens = kde.score_samples(X_plot)
                print('plot_data:', list(X_axis[:, 0]), list(np.exp(log_dens)), list(Y))
                ax.plot(X_plot[:, 0], Y, np.exp(log_dens),label="Int = '{0}'".format(name))
            # ax.text(6, 0.38, "N={0} points".format(N))

        ax.legend(loc='upper left')
        # ax.plot(X[:, 0], -0.005 - 0.01 * np.random.random(X.shape[0]), '+k')
        # ax.plot(X_time[:, 0], -0.005 - 0.01 * np.random.random(X.shape[0]), '+k')
        datetime_start = dt.datetime.strptime(str(date) + " 00:00:00", '%Y-%m-%d %H:%M:%S')
        datatime_1day = dt.timedelta(days=1)
        datetime_end = datetime_start + datatime_1day
        # ax.xaxis.set_major_formatter(mdate.DateFormatter('%H'))
        # ax.set_xlim([datetime_start, datetime_end])
        # ax.set_xticks(pd.date_range(datetime_start, datetime_end, freq='1H'))
        # # ax.set_xlim(0, 95)
        # ax.set_ylim(-0.02, 1.5)
        # fig.savefig("G:\plot_pic\\" + system_id + '-' + lane_no + '.png', dpi=75)
        plt.title('各路口报警数据核密度3d分布', fontsize=20)
        plt.savefig(r'..\alarm_predict\plot_pic\all_result_%s_3d.png' % self.date, dpi=300)
        plt.show()
        plt.close()

    def draw_surface_plot(self):
        plot_data = self.data
        X_plot = self.X_plot
        X_axis = self.X_axis
        date = self.date
        surface_X = np.zeros((TIME_POINT,INT_NUM))
        surface_Y = np.zeros((TIME_POINT,INT_NUM))
        surface_Z = np.zeros((TIME_POINT,INT_NUM))
        fig = plt.figure(figsize=(40, 30))
        ax = fig.gca(projection='3d')
        time_aixs = np.arange(0, 96, 12)
        ax.set_xticks(time_aixs)
        time_list = [str(i)[11:13] for i in X_axis[:,0]]
        ax.set_xticklabels([time_list[i] for i in time_aixs])
        print("set_xticks:",[str(i)[12:14] for i in X_axis[:,0]])
        keys = plot_data.keys()
        keys = list(keys)
        # print("keys", keys)
        # ax.set_zlim(0, 1000)
        ax.set_yticks([i * 5 for i in range(INT_NUM)])
        ax.set_yticklabels([keys[i] for i in range(INT_NUM)])

        for i in range(len(keys)):
            if i >= INT_NUM:
                break
            name = keys[i]
            X_time = plot_data[name][1]
            Y = np.array([i * 5 for m in range(len(X_plot))])
            print(i * 5 )
            X = plot_data[name][0]
            # print(X)
            # ax.fill(X_plot[:, 0], true_dens, fc='black', alpha=0.2,
            #         label='input distribution')
            for kernel in ['gaussian',
                           # 'tophat', 'epanechnikov'
                           ]:
                kde = KernelDensity(kernel=kernel, bandwidth=0.5).fit(X)
                log_dens = kde.score_samples(X_plot)
                print('plot_data:', list(X_axis[:, 0]), list(np.exp(log_dens)), list(Y))
                surface_X = X_plot[:, 0][:, np.newaxis]

                # if len(surface_Y) ==0:
                #     surface_Y = Y
                #     surface_Z = np.exp(log_dens)[:, np.newaxis]
                # else:
                surface_Y[:,i] = Y
                surface_Z[:,i] = np.exp(log_dens)
        # u = np.linspace(0, 2 * np.pi, 100)
        # v = np.linspace(0, np.pi, 100)
        # surface_X = 10 * np.outer(np.cos(u), np.sin(v))
        # surface_Y = 10 * np.outer(np.sin(u), np.sin(v))
        # surface_Z = 10 * np.outer(np.ones(np.size(u)), np.cos(v))
        print(surface_X.shape,'\n', surface_Y.shape,'\n',surface_Z.shape)
        plt.title('各路口报警数据核密度3d表面分布', fontsize=20)
        # print(surface_Y)
        # print(surface_Z)
        ax.plot_surface(surface_X, surface_Y, surface_Z,rstride=1, cstride=1, color='g')

        # ax.plot(X_plot[:, 0], Y, np.exp(log_dens))
            # ax.text(6, 0.38, "N={0} points".format(N))
        plt.savefig(r'..\alarm_predict\plot_pic\all_result_%s_surface.png' % self.date, dpi=300)
        plt.show()
        plt.close()

    def draw_hist_plot(self):
        plot_data = self.data
        X_plot = self.X_plot
        X_axis = self.X_axis
        date = self.date

        ax = plt.subplot(111)
        keys = plot_data.keys()
        time_aixs = np.arange(0, 96, 12)
        ax.set_xticks(time_aixs)
        time_list = [str(i)[11:13] for i in X_axis[:, 0]]
        ax.set_xticklabels([time_list[i] for i in time_aixs])
        print("set_xticks:", [str(i)[12:14] for i in X_axis[:, 0]])
        keys = list(keys)

        # print("keys", keys)
        # ax.set_zlim(0, 1000)
        # plt.hist(s, 10, normed=True)
        for i in range(len(keys)):
            name = keys[i]
            X_time = plot_data[name][1]
            Y = np.array([i * 5 for m in range(len(X_plot))])
            print(i * 5)
            X = plot_data[name][0]
            plt.hist(X, 24, normed=False, label=name)
            # print(X)
        # fig = plt.figure(figsize=(40, 30))
        # ax = fig.gca(projection='3d')
        plt.legend(loc='upper right')
        plt.show()

    def draw_divdate_plot(self):
        """绘制子分类核密度分布"""
        plot_data = self.data
        X_plot = self.X_plot
        X_axis = self.X_axis
        keys = list(plot_data.keys())
        for i in range(len(keys)):
            fig = plt.figure(figsize=(40, 30))
            ax = fig.gca(projection='3d')
            key_match_data = plot_data[keys[i]]
            date_keys = list(key_match_data.keys())
            time_aixs = np.arange(0, 96, 12)
            ax.set_xticks(time_aixs)
            time_list = [str(i)[11:13] for i in X_axis[:, 0]]
            ax.set_xticklabels([time_list[i] for i in time_aixs])
            print("set_xticks:", [str(i)[12:14] for i in X_axis[:, 0]])
            ax.set_yticks([i * 5 for i in range(len(date_keys))])
            ax.set_yticklabels([date_keys[i] for i in range(len(date_keys))])

            for index, value in enumerate(date_keys):
                Y = np.array([index * 5 for m in range(len(X_plot))])
                date = value
                X = key_match_data[date][0]
                X_time = key_match_data[date][1]
                # 计算核密度分布
                for kernel in ['gaussian',
                               # 'tophat', 'epanechnikov'
                               ]:
                    kde = KernelDensity(kernel=kernel, bandwidth=0.5).fit(X)
                    log_dens = kde.score_samples(X_plot)
                    # print('plot_data:', list(X_axis[:, 0]), list(np.exp(log_dens)), list(Y))
                    ax.plot(X_plot[:, 0], Y, np.exp(log_dens), linewidth=2, label=(GROUPCOLUMN+'_'+str(value)))
            ax.legend(loc='upper left')
            plt.title(str(keys[i],) + '号路口报警数据核密度分布', fontsize=20)
            plt.savefig(r'..\alarm_predict\plot_pic\all_result_3d_grouped_%s_%s.png' % (GROUPCOLUMN, str(keys[i])),
                        dpi= 150)
            plt.show()
            plt.close()

    def draw_2d_divdate_plot(self):
        """绘制子分类核密度分布"""
        plot_data = self.data
        X_plot = self.X_plot
        X_axis = self.X_axis
        keys = list(plot_data.keys())
        plot_data_save = []
        max_steps = len(keys)
        process_bar = ShowProcess(max_steps, 'OK')
        if self.plot_save:
            for i in range(len(keys)):
                process_bar.show_process()
                # ax = plt.subplot(331)
                key_match_data = plot_data[keys[i]]
                date_keys = list(key_match_data.keys())
                time_aixs = np.arange(0, int(24 * 60 / DRAW_INTERVAL), 12)
                # ax.set_xticks(time_aixs)
                time_list = [str(i)[11:13] for i in X_axis[:, 0]]
                for index, value in enumerate(date_keys):
                    plt_location = 330 + index + 1
                    week = value
                    X = key_match_data[week][0]
                    days = key_match_data[week][4]
                    avg_alarm = len(X) / days
                    X_new = key_match_data[week][2]
                    print(X_new)
                    new_alarm = len(X_new)
                    X_time = key_match_data[week][1]
                    int_name = key_match_data[week][3]
                    if len(X) > 0:
                        log_dens_his = self.kernel_predict(X)
                        log_dens_his_data = list(avg_alarm * np.exp(log_dens_his))
                    else:
                        log_dens_his = None
                        log_dens_his_data = None
                    # 计算核密度分布
                    # log_dens_his = self.kernel_predict(X)
                    total_width, n = 0.8, 2
                    width = total_width / n
                    log_dens_new = self.kernel_predict(X_new)
                    log_dens_new_data = list(new_alarm * np.exp(log_dens_new))
                    plot_data_save.append(
                        [keys[i], int_name, week_day(index), list(X_plot[:, 0]), log_dens_his_data ,log_dens_new_data])

        else:
            dir_path = r'..\alarm_predict\plot_pic%s' % dt.datetime.strftime(dt.datetime.now(), '%Y%m%d_%H%M')
            mkdir(dir_path)
            for i in range(len(keys)):
                process_bar.show_process()
                plt.figure(figsize=(30, 21))
                plt.title(str(keys[i], ) + '号路口一周报警数据核密度分布', fontsize=20)
                # ax = plt.subplot(331)
                key_match_data = plot_data[keys[i]]
                date_keys = list(key_match_data.keys())
                time_aixs = np.arange(0, int(24*60/DRAW_INTERVAL), 12)
                # ax.set_xticks(time_aixs)
                time_list = [str(i)[11:13] for i in X_axis[:, 0]]
                # ax.set_xticklabels([time_list[i] for i in time_aixs])
                # print("set_xticks:", [str(i)[12:14] for i in X_axis[:, 0]])
                # ax.set_yticks([i * 5 for i in range(len(date_keys))])
                # ax.set_yticklabels([date_keys[i] for i in range(len(date_keys))])

                for index, value in enumerate(date_keys):
                    plt_location = 330+index+1
                    ax = plt.subplot(plt_location)

                    # ax2 = ax.twinx()
                    plt.subplots_adjust(left=None, bottom=None, right=None, top=None,
                                        wspace=None, hspace=0.5)
                    ax.set_xticks(time_aixs)
                    ax.set_xticklabels([time_list[i] for i in time_aixs])
                    # print("set_xticks:", [str(i)[12:14] for i in X_axis[:, 0]])
                    # ax.set_yticks([i * 5 for i in range(len(date_keys))])
                    # ax.set_yticklabels([date_keys[i] for i in range(len(date_keys))])
                    # Y = np.array([index * 5 for m in range(len(X_plot))])
                    week = value

                    X = key_match_data[week][0]
                    X_new = key_match_data[week][2]
                    days = key_match_data[week][4]
                    avg_alarm = len(X) / days
                    if len(X) > 0:
                        log_dens_his = self.kernel_predict(X)
                        ax.plot(X_plot[:, 0], avg_alarm * np.exp(log_dens_his), '--', linewidth=1.5,
                                label=('历史平均KDE分布'), color='orange')
                    else:
                        continue
                    days = key_match_data[week][4]
                    avg_alarm = len(X)/days
                    X_new = key_match_data[week][2]
                    new_alarm = len(X_new)
                    X_time = key_match_data[week][1]
                    int_name = key_match_data[week][3]

                    total_width, n = 0.8, 2
                    width = total_width / n
                    # print('plot_data:', list(X_axis[:, 0]), list(np.exp(log_dens)), list(Y))
                    # ax2.bar(X_plot[:, 0], avg_alarm * np.exp(log_dens), width=width, label='历史平均报警概率', fc='orange')
                    log_dens_new = self.kernel_predict(X_new)
                    ax.plot(X_plot[:, 0], new_alarm*np.exp(log_dens_new), '-', linewidth=1.5, label=("最近一天KDE分布"),
                            color='darkcyan')
                    plot_data_save.append(
                        [keys[i], int_name, week_day(index), list(X_plot[:, 0]), list(avg_alarm * np.exp(log_dens_his)),
                         list(avg_alarm * np.exp(log_dens_new))])

                    # total_width, n = 0.8, 2
                    # width = total_width / n
                    # ax2.bar(X_plot[:, 0], new_alarm*np.exp(log_dens), width=width, label='最近一天平均报警概率', fc='darkcyan')
                    # ax.bar(X_plot[:, 0], avg_alarm*np.exp(log_dens))
                    ax.legend(loc='upper left')
                    # plt.xlabel(r'Time', frontsize=10)
                    # plt.ylabel(r'KDE', frontsize=10)
                    ax.set_title(str(keys[i], ) + '-' + int_name + '路口'+week_day(index) + 'KDE分布')
                    ax.set_xlabel(r'Time')
                    ax.set_ylabel(r'KDE')
                # ax.text(15, 1, str(keys[i], ) + '号路口一周报警数据核密度分布', fontsize=20)

                plt.savefig(dir_path + r'\all_result_2d_grouped_%s_%s.png' % (GROUPCOLUMN, str(keys[i])),
                            dpi=150)
                # plt.show()
                plt.close()
        return plot_data_save

    def save_2d_newalarm_KDE(self):
        """绘制子分类核密度分布"""
        plot_data = self.data
        X_plot = self.X_plot
        X_axis = self.X_axis
        keys = list(plot_data.keys())
        plot_data_save = []
        max_steps = len(keys)
        process_bar = ShowProcess(max_steps, 'OK')
        if self.plot_save:
            for i in range(len(keys)):
                process_bar.show_process()
                # ax = plt.subplot(331)
                key_match_data = plot_data[keys[i]]
                date_keys = list(key_match_data.keys())
                time_aixs = np.arange(0, int(24 * 60 / DRAW_INTERVAL), 12)
                # ax.set_xticks(time_aixs)
                time_list = [str(i)[11:13] for i in X_axis[:, 0]]
                for index, value in enumerate(date_keys):
                    plt_location = 330 + index + 1
                    week = value
                    X = key_match_data[week][0]
                    days = key_match_data[week][4]
                    avg_alarm = len(X) / days
                    X_new = key_match_data[week][2]
                    print(X_new)
                    new_alarm = len(X_new)
                    X_time = key_match_data[week][1]
                    int_name = key_match_data[week][3]
                    if len(X) > 0:
                        log_dens_his = self.kernel_predict(X)
                        log_dens_his_data = list(avg_alarm * np.exp(log_dens_his))
                    else:
                        log_dens_his = None
                        log_dens_his_data = None
                    # 计算核密度分布
                    # log_dens_his = self.kernel_predict(X)
                    total_width, n = 0.8, 2
                    width = total_width / n
                    log_dens_new = self.kernel_predict(X_new)
                    log_dens_new_data = list(new_alarm * np.exp(log_dens_new))
                    plot_data_save.append(
                        [keys[i], int_name, week_day(index), list(X_plot[:, 0]), log_dens_his_data, log_dens_new_data])

    def draw_active_3d_plot(self):
        pass
    # def draw_alarm_count(self):
    #     plot_data = self.data
    #     X_plot = self.X_plot
    #     keys = list(plot_data.keys())
    #     X_axis = self.X_axis
    #     for i in range(len(keys)):
    #         fig = plt.figure(figsize=(40, 30))
    #         ax = fig.gca(projection='3d')
    #         key_match_data = plot_data[keys[i]]
    #         date_keys = list(key_match_data.keys())
    #         time_aixs = np.arange(0, 96, 12)
    #         ax.set_xticks(time_aixs)
    #         print(X_axis[:, 0])
    #         time_list = [str(i)[11:13] for i in X_axis[:, 0]]
    #         print("time_list", time_list)
    #         ax.set_xticklabels([time_list[i] for i in time_aixs])
    #         print("set_xticks:", [str(i)[12:14] for i in X_axis[:, 0]])
    #         ax.set_yticks([i * 5 for i in range(len(date_keys))])
    #         ax.set_yticklabels([date_keys[i] for i in range(len(date_keys))])
    #         for index, value in enumerate(date_keys):
    #             Y = np.array([index * 5 for m in range(len(X_plot))])
    #             date = value
    #             X = key_match_data[date][0]
    #             X_time = key_match_data[date][1]
    #             for kernel in ['gaussian',
    #                            # 'tophat', 'epanechnikov'
    #                            ]:
    #                 kde = KernelDensity(kernel=kernel, bandwidth=0.5).fit(X)
    #                 log_dens = kde.score_samples(X_plot)
    #                 # print('plot_data:', list(X_axis[:, 0]), list(np.exp(log_dens)), list(Y))
    #                 ax.plot(X_plot[:, 0], Y, len(X)*np.exp(log_dens))
    #         ax.legend(loc='upper left')
    #         # plt.savefig(r'..\alarm_predict\plot_pic\all_result_%s_3d.png' % self.date, dpi=300)
    #         plt.show()
    #         plt.close()

    def draw_2d_dir_plot(self):
        """绘制子分类核密度分布"""
        plot_data = self.data
        X_plot = self.X_plot
        X_axis = self.X_axis
        int_list = list(plot_data.keys())
        plot_data_save = []
        max_steps = len(int_list)
        process_bar = ShowProcess(max_steps, 'OK')
        if self.plot_save:
            for i in range(len(int_list)):
                process_bar.show_process()
                # ax = plt.subplot(331)
                int_match_data = plot_data[int_list[i]]
                weekdays = list(int_match_data.keys())
                time_aixs = np.arange(0, int(24 * 60 / DRAW_INTERVAL), 12)
                # ax.set_xticks(time_aixs)
                time_list = [str(i)[11:13] for i in X_axis[:, 0]]
                for index_week, value in enumerate(weekdays):
                    plt_location = 330 + index_week + 1
                    week_match_data = int_match_data[weekdays[index_week]]
                    week = value
                    dir_desc = list(week_match_data.keys())
                    for index_dir, value in enumerate(dir_desc):

                        dir = value
                        X = week_match_data[dir][0]
                        days = week_match_data[dir][4]
                        avg_alarm = len(X) / days
                        X_new = week_match_data[dir][2]
                        print(X_new)
                        new_alarm = len(X_new)
                        X_time = week_match_data[dir][1]
                        int_name = week_match_data[dir][3]
                        if len(X) > 0:
                            log_dens_his = self.kernel_predict(X)
                            log_dens_his_data = list(avg_alarm * np.exp(log_dens_his))
                        else:
                            log_dens_his = None
                            log_dens_his_data = None
                        # 计算核密度分布
                        # log_dens_his = self.kernel_predict(X)
                        total_width, n = 0.8, 2
                        width = total_width / n
                        log_dens_new = self.kernel_predict(X_new)
                        log_dens_new_data = list(new_alarm * np.exp(log_dens_new))
                        plot_data_save.append(
                            [int_list[i], int_name, week_day(index_week), dir,  list(X_plot[:, 0]), log_dens_his_data, log_dens_new_data])

        else:
            dir_path = r'..\alarm_predict\plot_pic%s' % dt.datetime.strftime(dt.datetime.now(), '%Y%m%d_%H%M')
            mkdir(dir_path)
            for i in range(len(int_list)):

                process_bar.show_process()
                # ax = plt.subplot(331)
                int_match_data = plot_data[int_list[i]]
                weekdays = list(int_match_data.keys())
                time_aixs = np.arange(0, int(24 * 60 / DRAW_INTERVAL), 12)
                # ax.set_xticks(time_aixs)
                time_list = [str(i)[11:13] for i in X_axis[:, 0]]
                for index_week, value in enumerate(weekdays):
                    plt_location = 330 + index_week + 1
                    week_match_data = int_match_data[weekdays[index_week]]
                    week = value
                    dir_desc = list(week_match_data.keys())
                    print(dir_desc)
                    plt.figure(figsize=(30, 21))
                    for index_dir, value in enumerate(dir_desc):

                        plt_location = 330 + index_dir + 1
                        ax = plt.subplot(plt_location)
                        plt.subplots_adjust(left=None, bottom=None, right=None, top=None,
                                            wspace=None, hspace=0.5)
                        ax.set_xticks(time_aixs)
                        ax.set_xticklabels([time_list[i] for i in time_aixs])
                        dir = value
                        X = week_match_data[dir][0]
                        days = week_match_data[dir][4]
                        X_new = week_match_data[dir][2]
                        # print(X_new)
                        new_alarm = len(X_new)
                        X_time = week_match_data[dir][1]
                        int_name = week_match_data[dir][3]
                        if days > 0:
                            avg_alarm = len(X) / days
                            log_dens_his = self.kernel_predict(X)
                            log_dens_his_data = list(avg_alarm * np.exp(log_dens_his))
                            ax.plot(X_plot[:, 0], log_dens_his_data, '--', linewidth=1.5,
                                    label=('历史平均KDE分布,日均报警量:%s'%avg_alarm), color='orange')
                        else:
                            log_dens_his = None
                            log_dens_his_data = None
                        # 计算核密度分布
                        # log_dens_his = self.kernel_predict(X)
                        total_width, n = 0.8, 2
                        width = total_width / n
                        log_dens_new = self.kernel_predict(X_new)
                        log_dens_new_data = list(new_alarm * np.exp(log_dens_new))
                        ax.plot(X_plot[:, 0], log_dens_new_data, '-', linewidth=1.5, label=("最近一天KDE分布,日报警量:%s"%new_alarm),
                                color='darkcyan')
                        kde_acc = []
                        kde_reg = []
                        print('-----------------------------------------')

                        for index, value in enumerate(log_dens_new_data):
                            if value > KDE_RANGE*log_dens_his_data[index]:
                                kde_acc.append(value)
                                kde_reg.append(0)
                            else:
                                kde_acc.append(0)
                                kde_reg.append(value)

                        ax.scatter(X_plot[:, 0], kde_acc, marker ='^',
                                )
                        ax.scatter(X_plot[:, 0], kde_reg, marker ='*',
                                )

                        plot_data_save.append(
                            [int_list[i], int_name, week_day(index_week), dir, list(X_plot[:, 0]), log_dens_his_data,
                             log_dens_new_data])

                        # total_width, n = 0.8, 2
                        # width = total_width / n
                        # ax2.bar(X_plot[:, 0], new_alarm*np.exp(log_dens), width=width, label='最近一天平均报警概率', fc='darkcyan')
                        # ax.bar(X_plot[:, 0], avg_alarm*np.exp(log_dens))
                        ax.legend(loc='upper left')
                        # plt.xlabel(r'Time', frontsize=10)
                        # plt.ylabel(r'KDE', frontsize=10)
                        ax.set_title(str(int_list[i], ) + '-' + int_name + '路口' + week_day(index_week) + dir + 'KDE分布')
                        ax.set_xlabel(r'Time')
                        ax.set_ylabel(r'KDE')
                    # ax.text(15, 1, str(keys[i], ) + '号路口一周报警数据核密度分布', fontsize=20)
                    plt.savefig(dir_path + r'\all_result_2d_grouped_%s_%s_%s.png' % (GROUPCOLUMN, str(int_list[i]), week_day(index_week)),
                                dpi=150)
                    # plt.show()
                    plt.close()



    def draw_2d_alarmcount(self):
        """统计各路口每天的报警量"""
        plot_data = self.data
        X_plot = self.X_plot
        keys = list(plot_data.keys())
        X_axis = self.X_axis
        for i in range(len(keys)):
            ax = plt.subplot(111)
            key_match_data = plot_data[keys[i]]
            date_keys = list(key_match_data.keys())
            # time_aixs = np.arange(0, 96, 12)
            # ax.set_xticks(time_aixs)
            # print(X_axis[:, 0])
            # time_list = [str(i)[11:13] for i in X_axis[:, 0]]
            # print("time_list", time_list)
            # ax.set_xticklabels([time_list[i] for i in time_aixs])
            # print("set_xticks:", [str(i)[12:14] for i in X_axis[:, 0]])
            ax.set_xticks([i for i in range(len(date_keys))])
            ax.set_xticklabels([date_keys[i] for i in range(len(date_keys))])
            plt.xticks(rotation=45)
            for index, value in enumerate(date_keys):
                # Y = np.array([index * 5 for m in range(len(X_plot))])
                date = value
                X_data = key_match_data[date][0]

                avg_alarm = len(X)
                X = index
                Y = len(X_data)
                ax.bar(X, Y)
            ax.legend(loc='upper left')
            # plt.savefig(r'..\alarm_predict\plot_pic\all_result_%s_3d.png' % self.date, dpi=300)
            plt.show()
            plt.close()


    # def draw_3d_weekmean(self):
    #     """周一至周日各路口报警核密度分布"""
    #     plot_data = self.data
    #     X_plot = self.X_plot
    #     X_axis = self.X_axis
    #     keys = list(plot_data.keys())
    #     for i in range(len(keys)):
    #         fig = plt.figure(figsize=(40, 30))
    #         ax = fig.gca(projection='3d')
    #         key_match_data = plot_data[keys[i]]
    #         week_keys = list(key_match_data.keys())
    #         time_aixs = np.arange(0, 96, 12)
    #         ax.set_xticks(time_aixs)
    #         time_list = [str(i)[11:13] for i in X_axis[:, 0]]
    #         ax.set_xticklabels([time_list[i] for i in time_aixs])
    #         print("set_xticks:", [str(i)[12:14] for i in X_axis[:, 0]])
    #         keys = plot_data.keys()
    #         keys = list(keys)
    #         # print("keys", keys)
    #         # ax.set_zlim(0, 1000)
    #         # print("week_keys", week_keys)
    #         ax.set_yticks([i * 5 for i in range(len(week_keys))])
    #         ax.set_yticklabels([week_keys[i] for i in range(len(week_keys))])
    #         for index, value in enumerate(week_keys):
    #             Y = np.array([index * 5 for m in range(len(X_plot))])
    #             date = value
    #             X = key_match_data[date][0]
    #             X_time = key_match_data[date][1]
    #             for kernel in ['gaussian',
    #                            # 'tophat', 'epanechnikov'
    #                            ]:
    #                 kde = KernelDensity(kernel=kernel, bandwidth=0.5).fit(X)
    #                 log_dens = kde.score_samples(X_plot)
    #                 # print('plot_data:', list(X_axis[:, 0]), list(np.exp(log_dens)), list(Y))
    #                 ax.plot(X_plot[:, 0], Y, np.exp(log_dens))
    #         ax.legend(loc='upper left')
    #         # plt.savefig(r'..\alarm_predict\plot_pic\all_result_%s_3d.png' % self.date, dpi=300)
    #         plt.show()
    #         plt.close()


class AlarmClassify():
    def __init__(self, alarm_data, group_columns):
        """alarm_data为根据路口，方向，星期几分类后的数据
           group_columns为分类标签字符表示
        """

        self.alarm_data = alarm_data
        self.group_columns = group_columns

    def kernel_predict(self, X):
        X_plot = self.X_plot
        for kernel in ['gaussian',
                       # 'tophat', 'epanechnikov'
                       ]:
            kde = KernelDensity(kernel=kernel, bandwidth=0.5).fit(X)
            log_dens = kde.score_samples(X_plot)
        return log_dens

    def call_kde(self):
        alarm_data = self.alarm_data

    def data_create(self):
        alarm_data = self.alarm_data
        grouped = alarm_data.groupby(['scats_id', 'weekday', 'dir_desc'])
        plot_data = {}
        # print(grouped)

        # 数据分组-按日期分组绘图

        for (k1, k2, k3), group in grouped:
            if plot_data.get(k1):
                pass
            else:
                plot_data[k1] = {}
            if plot_data[k1].get(k2):
                pass
            else:
                plot_data[k1][k2] = {}
            # 历史数据
            X = group['time'][group['date'] != max(list(group['date']))][:, np.newaxis]
            # X = np.array(result)[:, np.newaxis]
            days = len(group['date'].unique())-1
            # 最新数据
            X_new = group['time'][group['date'] == max(list(group['date']))][:, np.newaxis]
            X_time = group['time_point'][:, np.newaxis]
            int_name = list(set(list(group['inter_name'])))
            plot_data[k1][k2][k3] = [X, X_time, X_new, int_name[0], days]
        print(plot_data)

        datetime_start = dt.datetime.strptime("2018-09-26 00:00:00", "%Y-%m-%d %H:%M:%S")
        datatime_1day = dt.timedelta(hours=23.75)
        datetime_end = datetime_start + datatime_1day
        X_axis = pd.date_range(datetime_start, datetime_end, freq=str(DRAW_INTERVAL * 60) + 's')[:, np.newaxis]
        X_plot = np.linspace(0, int(24 * 60 / DRAW_INTERVAL) - 1, int(24 * 60 / DRAW_INTERVAL))[:, np.newaxis]
        print(X_axis)
        K1 = Kernal(plot_data, X_plot, X_axis)
        return K1


def mkdir(path):
    folder = os.path.exists(path)

    if not folder:  # 判断是否存在文件夹如果不存在则创建为文件夹
        os.makedirs(path)  # makedirs 创建文件时如果路径不存在会创建这个路径
        print("---  new folder...  ---")
        print("---  OK  ---")
    else:
        print("---  There is this folder!  ---")


def week_day(num):
    weekday_list = ['周一','周二','周三','周四','周五','周六','周日']
    return weekday_list[num]


def draw_group_date(data, group_column):
    grouped = data.groupby('scats_id')
    plot_data = {}
    # 数据分组-按日期分组绘图
    for name, group in grouped:
        plot_data[name] = {}
        grouped2 = group.groupby(group_column)
        for date, group2 in grouped2:
            # 历史数据
            X = group2['time'][group2['date'] != max(list(group2['date']))][:, np.newaxis]
            # X = np.array(result)[:, np.newaxis]
            days = len(group2['date'].unique())
            # 最新数据
            X_new = group2['time'][group2['date'] == max(list(group2['date']))][:, np.newaxis]
            X_time = group2['time_point'][:, np.newaxis]
            int_name = list(set(list(group2['inter_name'])))
            plot_data[name][date] = [X, X_time, X_new, int_name[0], days]

    datetime_start = dt.datetime.strptime("2018-09-26 00:00:00", "%Y-%m-%d %H:%M:%S")
    datatime_1day = dt.timedelta(hours=23.75)
    datetime_end = datetime_start + datatime_1day
    X_axis = pd.date_range(datetime_start, datetime_end, freq=str(DRAW_INTERVAL*60) + 's')[:, np.newaxis]
    X_plot = np.linspace(0, int(24*60/DRAW_INTERVAL)-1, int(24*60/DRAW_INTERVAL))[:, np.newaxis]
    print(X_axis)
    K = Kernal(plot_data, X_plot, X_axis)

    return K
    # 绘制


def draw_group_int(data,date):
    grouped = data.groupby('scats_id')
    def alarm_time_change(data):
        alarm_time = data['time_point']
        plot_data = []
        plot_time = []
        for i in range(len(data)):
            alarm_time = data.iloc[i, 2]
            print(alarm_time)
            # time2datetime = dt.datetime.strptime(str(alarm_time), '%Y-%m-%d %H:%M:%S')
            time2datetime = alarm_time
            time2min = time2datetime.hour * 60 + time2datetime.minute
            x_plot = time2min / 15
            # x_plot = alarm_time
            # time2time = dt.datetime.strptime(alarm_time, '%Y-%m-%d %H:%M:%S')
            # time2date = dt.datetime.strptime(alarm_time, '%Y-%m-%d')
            # time2min = (time2time-time2date).min
            print(time2min)
            plot_data.append(x_plot)
            plot_time.append(alarm_time)
        return plot_data, plot_time
    plot_data = {}
    for name, group in grouped:
        print(name)
        result, plot_time = alarm_time_change(group)
        X = np.array(result)[:, np.newaxis]
        X_time = np.array(plot_time)[:, np.newaxis]
        plot_data[name] = [X, X_time]

    datetime_start = dt.datetime.strptime(str(date) + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    datatime_1day = dt.timedelta(hours=23.75)
    datetime_end = datetime_start + datatime_1day
    X_axis = pd.date_range(datetime_start, datetime_end, freq='0.H')[:, np.newaxis]
    X_plot = np.linspace(0, 95, 96)[:, np.newaxis]
    print(X_plot)
    # X_axis =np.array( [str(i)[12:] for i in X_axis])[:, np.newaxis]
    K1 = Kernal(plot_data, X_plot, X_axis, date)
    return K1


def dir_alarm_regulartion(data,group):
    A = AlarmClassify(data,group)
    K = A.data_create()
    K.plot_save = True
    K.draw_2d_dir_plot()


def resolve_alarm_data(data, date=None):
    if date is not None:
        K = draw_group_int(data, date)
        # 绘制2d单日路口核密度折线图
        # K1.draw_2d_plot()
        # 绘制3d单日路口核密度折线图
        # K1.draw_3d_plot()
        # 绘制3d单日路口核密度封闭表面图
        K.draw_surface_plot()

    else:
        # 计算日期，时间，星期
        data['date']= data['time_point'].apply(lambda x: x.date() if x else None)
        data['time'] = data['time_point'].apply(lambda x: (x.hour*60 + x.minute)/DRAW_INTERVAL if x else None)
        data['weekday'] = data['time_point'].apply(lambda x: x.weekday()+1 if x else None)
        # 子分类 = GROUPCOLUMN
        group_column = GROUPCOLUMN
        # data_select = data[data['inter_id']== '14LHD097JA0']
        # print(data_select)
        # 分方向区分
        # dir_alarm_regulartion(data,None)
        every_day_kde_save(data)
        sys.exit(0)
        # K = draw_group_date(data, group_column)
        # 绘制子分类的核密度分布
        # K.plot_save = True

        # plot_data = K.draw_2d_divdate_plot()
        if K.plot_save:
            resolve_plot_data = []
            for data in plot_data:
                site_id = data[0]
                int_name = data[1]
                week_day = data[2]
                time_label = data[3]
                kde_alarm_his = data[4]
                kde_alarm_new = data[5]
                def check_regular_alarm(alarm_his,alarm_new):
                    pass
                def check_kde(list_kde, num):
                    if list_kde[num] <= 0.1:
                        q_kde_alarm = 0
                    else:
                        q_kde_alarm = round(list_kde[num], 1)
                    return q_kde_alarm
                for num in range(len(time_label)):
                    time_stamp = time_label[num]*DRAW_INTERVAL
                    hour = int(math.floor(time_stamp / 60))
                    min = int(math.floor(time_stamp % 60))
                    date = dt.datetime.now().date()
                    q_time = dt.time(hour, min, 0, 0)
                    q_datetime = dt.datetime.strptime(str(date)+' '+str(q_time) ,'%Y-%m-%d %H:%M:%S')
                    # print(q_datetime)
                    q_kde_alarm_his = check_kde(kde_alarm_his, num)
                    q_kde_alarm_new = check_kde(kde_alarm_new, num)
                    check_regular_alarm(q_kde_alarm_his, q_kde_alarm_new)
                    resolve_plot_data.append([site_id, int_name, week_day, q_datetime, q_kde_alarm_his, q_kde_alarm_new])
                    # time_stamp = dt.datetime.fromtimestamp(time_stamp)
                    # if q_kde_alarm_his > 0 or q_kde_alarm_new >0:
                    #     print([site_id, int_name, week_day, q_time, q_kde_alarm_his, q_kde_alarm_new])
            alarm_plot_data_save(resolve_plot_data,PLOT_DATA_SAVE)


        # plot_data_df = pd.DataFrame(plot_data,columns=['site_id','int_name', 'week_day','time_label','kde_alarm' ])
        # plot_data_df.to_csv('alarm_plot_data.csv')

        # 统计每个子分类的报警数
        # K.draw_2d_alarmcount()

    return None


def every_day_kde_save(data):
    alarm_data = data
    grouped = alarm_data.groupby(['scats_id', 'date', 'dir_desc'])
    plot_data = {}
    # print(grouped)
    # 数据分组-按日期分组绘图
    for (k1, k2, k3), group in grouped:
        # 历史数据
        if plot_data.get(k1):
            pass
        else:
            plot_data[k1] = {}
        if plot_data[k1].get(k2):
            pass
        else:
            plot_data[k1][k2] = {}
        # 历史数据
        X = group['time'][:, np.newaxis]
        # X = np.array(result)[:, np.newaxis]
        days = len(group['date'].unique())
        # 最新数据
        X_new = None
        X_time = group['time_point'][:, np.newaxis]
        int_name = list(set(list(group['inter_name'])))
        plot_data[k1][k2][k3] = [X, X_time, X_new, int_name[0], days]

    datetime_start = dt.datetime.strptime("2018-09-26 00:00:00", "%Y-%m-%d %H:%M:%S")
    datatime_1day = dt.timedelta(hours=23.75)
    datetime_end = datetime_start + datatime_1day
    X_axis = pd.date_range(datetime_start, datetime_end, freq=str(DRAW_INTERVAL * 60) + 's')[:, np.newaxis]
    X_plot = np.linspace(0, int(24 * 60 / DRAW_INTERVAL) - 1, int(24 * 60 / DRAW_INTERVAL))[:, np.newaxis]
    K1 = Kernal(plot_data, X_plot, X_axis)
    K1.plot_save = True
    plot_data = K1.save_everyday_kde()
    print(plot_data)

    if K1.plot_save:
        resolve_plot_data = []
        for data in plot_data:
            site_id = data[0]
            int_name = data[1]
            date = data[2]
            dir_desc = data[3]
            time_label = data[4]
            kde_alarm_his = data[5]
            # kde_alarm_new = data[5]

            def check_kde(list_kde, num):
                if list_kde[num] <= 0.1:
                    q_kde_alarm = 0
                else:
                    q_kde_alarm = round(list_kde[num], 1)
                return q_kde_alarm

            for num in range(len(time_label)):
                time_stamp = time_label[num] * DRAW_INTERVAL
                hour = int(math.floor(time_stamp / 60))
                min = int(math.floor(time_stamp % 60))
                # date = dt.datetime.now().date()
                q_time = dt.time(hour, min, 0, 0)
                q_datetime = dt.datetime.strptime(str(date) + ' ' + str(q_time), '%Y-%m-%d %H:%M:%S')

                # print(q_datetime)
                q_kde_alarm_his = check_kde(kde_alarm_his, num)
                # q_kde_alarm_new = check_kde(kde_alarm_new, num)
                # check_regular_alarm(q_kde_alarm_his, q_kde_alarm_new)
                weekday = q_datetime.weekday()
                resolve_plot_data.append([site_id, int_name, dir_desc, weekday, q_time, q_kde_alarm_his, str(date)])
                # time_stamp = dt.datetime.fromtimestamp(time_stamp)
                # if q_kde_alarm_his > 0 or q_kde_alarm_new >0:
                #     print([site_id, int_name, week_day, q_time, q_kde_alarm_his, q_kde_alarm_new])
        alarm_plot_data_save(resolve_plot_data, PLOT_DATA_SAVE2, KDE_STATISTIC)

    return


def alarm_plot_data_save(plot_data, sql1, sql2=None):
    """发送报警数据"""

    db = Postgres()
    db.db_conn()
    cr = db.cr
    conn = db.conn
    for i in plot_data:
        try:
            cr.execute(sql1, i)
        except Exception as e:
            print(e)
            pass
        conn.commit()

    if sql2:
        local_time = dt.datetime.now()
        today = local_time.date()
        last_month = (local_time - dt.timedelta(days=30)).date()
        try:
            cr.execute(sql2 % str(last_month))
        except Exception as e:
            print(e)
            pass
        conn.commit()
    db.db_close()


def main(all_alarm, date=None):
    alarm_match = Alarm_match()
    single_alarm_analyze(alarm_match, date=None)
    # area_alarm_analyze(alarm_match)


def single_alarm_analyze(alarm_match, date=None):
    if date is not None:
        alarm_data = alarm_match.call_new_alarm_data()
        print(alarm_data)
        resolve_alarm_data(alarm_data, date)
        all_alarm[date] = alarm_data
    else:
        alarm_data = alarm_match.call_new_alarm_data()
        resolve_alarm_data(alarm_data)
        pass


def area_alarm_analyze(alarm_match):
    data = alarm_match.call_new_alarm_data()
    data['date'] = data['time_point'].apply(lambda x: x.date() if x else None)
    data['time'] = data['time_point'].apply(lambda x: math.floor((x.hour * 60 + x.minute)) if x else None)
    data['weekday'] = data['time_point'].apply(lambda x: x.weekday() + 1 if x else None)


    data_dec = data[['inter_id', 'inter_name', 'coors', 'time_point', 'scats_id', 'date', 'time', 'weekday']]
    grouped_weekday = data_dec.groupby('weekday')
    for name1, group1 in grouped_weekday:
        weekday = name1
        grouped_time = group1.groupby('time')
        for name2, group2 in grouped_time:
            time = name2
            site_id = group2['scats_id'].drop_duplicates()
            print(site_id)





    print(data_dec)

    pass



if __name__ == '__main__':

    sdate = '2018-09-01'
    edate = '2018-09-26'
    date_list = pd.date_range('2018-09-01', '2018-09-18', freq='1D')
    all_alarm = {}

    # 遍历每一天画图
    # for i in date_list:
    #     DATETIME = '%s;%s;00:00:00;23:59:00' % (i.date(), i.date())
    #     print(DATETIME)
    #     main(all_alarm, i.date())

    # 取时间区间内所有数据画图
    DATETIME = '%s;%s;00:00:00;23:59:59' % (sdate, edate)
    main(all_alarm)

