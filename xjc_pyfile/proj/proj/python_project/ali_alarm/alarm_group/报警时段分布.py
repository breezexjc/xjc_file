#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import psycopg2 as pg
import time
import datetime
from ali_alarm import GlobalContent

def call_alarm():
    try:
        conn = pg.connect(database=GlobalContent.pg_database72_research['database'],
                          user=GlobalContent.pg_database72_research['user'],
                          password=GlobalContent.pg_database72_research['password'],
                          host=GlobalContent.pg_database72_research['host'],
                          port=GlobalContent.pg_database72_research['port'])
    except Exception as e:
        print(e)
    else:
        cr_pg = conn.cursor()
        sql ="""select rdsectid,
        round(count(case when "上游报警时间" < '09:30:00' and "上游报警时间" >= '06:30:00'  then rdsectid end )/count(rdsectid)::NUMERIC,2)as "早高峰",
        round(count(case when "上游报警时间" >= '09:30:00' and "上游报警时间" <= '12:00:00' then rdsectid end )/count(rdsectid)::NUMERIC,2)as "早平峰",
        round(count(case when "上游报警时间" >= '12:00:00' and "上游报警时间" <= '16:00:00' then rdsectid end )/count(rdsectid)::NUMERIC,2)as "晚平峰",
        round(count(case when "上游报警时间" >= '16:00:00' and "上游报警时间" <= '20:00:00' then rdsectid end )/count(rdsectid)::NUMERIC,2)as "晚高峰",
        count(rdsectid) as "总关联报警次数" from alarm_connect_reson2 GROUP BY rdsectid
        order by rdsectid"""
        cr_pg.execute(sql)
        result = cr_pg.fetchall()
        conn.close()
        print(result)
        return result


def draw_conn(data):
    import numpy as np
    from sklearn.cluster import KMeans

    # data_conn_num = [i for i ]

    # 假如我要构造一个聚类数为3的聚类器
    estimator = KMeans(n_clusters=3)  # 构造聚类器
    estimator.fit(data)  # 聚类
    # km_cluster = KMeans(n_clusters=num_clusters, max_iter=300, n_init=40, \
    #                     init='k-means++', n_jobs=-1)
    label_pred = estimator.labels_  # 获取聚类标签
    centroids = estimator.cluster_centers_  # 获取聚类中心
    inertia = estimator.inertia_  # 获取聚类准则的总和



if __name__ =="__main__":
    date = call_alarm()
    draw_conn(date)