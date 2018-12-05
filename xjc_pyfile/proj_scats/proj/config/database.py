#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
author: xjc
version: V20181031
project：单例模式数据库类
"""

import psycopg2
import pandas as pd
import cx_Oracle

# pg_inf = {'database': "signal_specialist",'user': "postgres",'password': "postgres",
#           'host': "192.168.20.56",'port': "5432"}


# pg_inf = {'database': "research",'user': "postgres",'password': "postgres",
#           'host': "192.168.20.45",'port': "5432"}
# OracleUser = 'SIG_OPT_ADMIN/admin@192.168.20.56/orcl'
OracleUser = 'enjoyor/admin@33.83.100.139/orcl'
demo_pg_inf = {'database': "signal_specialist", 'user': "django", 'password': "postgres",
               'host': "33.83.100.145", 'port': "5432"}


# demo_pg_inf = {'database': "signal_specialist",'user': "postgres",'password': "postgres",
#           'host': "33.83.100.145",'port': "5432"}


class Postgres(object):
    instance = None

    def __init__(self, pg_inf=demo_pg_inf):
        """
        :param pg_inf: 传入链接信息
        """
        self.pg_inf = pg_inf
        self.ip = self.pg_inf['host']
        self.database = self.pg_inf['database']
        self.port = self.pg_inf['port']
        self.account = self.pg_inf['user']
        self.password = self.pg_inf['password']
        self.conn = None
        self.cr = None

    def __del__(self):
        self.db_close()

    @classmethod
    def get_instance(cls, pg_inf=demo_pg_inf):
        if cls.instance:
            return cls.instance
        else:
            obj = cls(pg_inf=pg_inf)
            cls.instance = obj
            return obj

    def db_conn(self):
        if self.conn is not None:
            try:
                self.db_close()
            except Exception as e:
                print(e)
            pass
        try:
            self.conn = psycopg2.connect(database=self.database, user=self.account, password=self.password,
                                         host=self.ip,
                                         port=self.port)
        except Exception as e:
            print(self.ip + "Postgres connect failed", e)
            return self.conn, self.cr
        else:
            # print(self.ip +"Postgres connect succeed")
            self.cr = self.conn.cursor()
            return self.conn, self.cr

    def db_close(self):
        if self.conn is not None:
            try:
                self.cr.close()
                self.conn.close()
            except Exception as e:
                print('db_close', e)
            finally:
                self.conn = None
                self.cr = None

    def call_pg_data(self, sql, pg_inf=None, fram=None):
        """
        :param sql: 需要执行的SQL语句
        :param pg_inf:如果要更改链接地址
        :param fram: 是否返回DataFrame格式数据，为True则返回DataFrame
        :return: 默认返回元组格式数据
        """
        result = None

        def tuple2frame(result, index):
            column_name = []
            for i in range(len(index)):
                index_name = index[i][0]
                column_name.append(index_name)
            result = pd.DataFrame(result, columns=column_name)
            return result

        if pg_inf is not None:
            self.pg_inf = pg_inf
            self.ip = self.pg_inf['host']
            self.database = self.pg_inf['database']
            self.port = self.pg_inf['port']
            self.account = self.pg_inf['user']
            self.password = self.pg_inf['user']
            self.db_close()
        else:
            pass

        cr = self.cr
        conn = self.conn
        if conn is None:
            conn, cr = self.db_conn()
        else:
            pass
        if cr:
            try:
                cr.execute(sql)
            except Exception as e:
                conn.commit()
                print("call pg error", e)
            else:
                # print("call pg success!")
                index = cr.description
                result = cr.fetchall()
                conn.commit()
                # self.db_close()
                if result is not None:
                    if fram is not None:
                        fresult = tuple2frame(result, index)
                        return fresult
                    else:
                        return result
                else:
                    return None
        else:
            print("database connect failed")
            # sys.exit(0)

    def send_pg_data(self, sql, data=None, pg_inf=None, sql_delete=None, key_location=None):
        """
        :param sql: 插入数据SQL语句
        :param data: 插入数据，列表或元组等可迭代格式
        :param pg_inf: 更换链接信息？
        :param sql_delete: 需要删除数据的SQL语句
        :param key_location: 主键位置
        :return: 无返回
        """
        error_num = 0
        correct_num = 0
        repet_num = 0

        if pg_inf is not None:
            self.pg_inf = pg_inf
            self.ip = self.pg_inf['host']
            self.database = self.pg_inf['database']
            self.port = self.pg_inf['port']
            self.account = self.pg_inf['user']
            self.password = self.pg_inf['user']
        else:
            pass
        cr = self.cr
        conn = self.conn
        if conn is None:
            conn, cr = self.db_conn()
        else:
            pass
        if cr:
            for i in data:
                try:
                    cr.execute(sql, i)
                except psycopg2.IntegrityError as e:
                    # print(e)
                    error_num += 1
                    repet_num += 1
                    # 主键去重
                    if sql_delete is not None and key_location is not None:
                        try:
                            conn.commit()
                            key = [i[m] for m in key_location]
                            key = tuple(key)
                            sql_delete2 = sql_delete % key
                            print(sql_delete2)
                            cr.execute(sql_delete2)
                            conn.commit()
                            cr.execute(sql, i)
                            conn.commit()
                        except Exception as e:
                            print('delete error', e)
                        else:
                            print('delete success!', e)
                except Exception as e:
                    print("send pg error", e)
                    error_num += 1
                else:
                    correct_num += 1
                finally:
                    conn.commit()
            print("插入成功:%s,插入失败:%s,其中主键重复：%s" % (correct_num, error_num, repet_num))
            self.db_close()
        else:
            print("database connect failed")
            # return pd.DataFrame({})

    def send_pg_data_many(self, sql, data=None, pg_inf=None):
        if pg_inf is not None:
            self.pg_inf = pg_inf
            self.ip = self.pg_inf['host']
            self.database = self.pg_inf['database']
            self.port = self.pg_inf['port']
            self.account = self.pg_inf['user']
            self.password = self.pg_inf['user']
        else:
            pass
        cr = self.cr
        conn = self.conn
        if conn is None:
            conn, cr = self.db_conn()
        else:
            pass
        assert type(data) is list or tuple
        if cr:
            try:
                cr.executemany(sql, data)
            except Exception as e:
                conn.commit()
            finally:
                conn.commit()
                self.db_close()
        else:
            print("database connect failed")
            # return pd.DataFrame({})

    def execute(self, sql, pg_inf=None):
        error_num = 0
        correct_num = 0
        repet_num = 0

        if pg_inf is not None:
            self.pg_inf = pg_inf
            self.ip = self.pg_inf['host']
            self.database = self.pg_inf['database']
            self.port = self.pg_inf['port']
            self.account = self.pg_inf['user']
            self.password = self.pg_inf['user']
            self.db_close()
        else:
            pass
        cr = self.cr
        conn = self.conn
        if conn is None:
            conn, cr = self.db_conn()
        else:
            pass
        if cr:
            try:
                cr.execute(sql)
            except Exception as e:
                print("execute error", e)
            # print("插入成功:%s,插入失败:%s,其中主键重复：%s" % (correct_num, error_num, repet_num))
            # self.db_close()
            finally:
                conn.commit()
        else:
            print("database connect failed")
            # return pd.DataFrame({})


class Oracle(object):
    instance = None

    def __init__(self, user_inf=OracleUser):
        self.conn_inf = user_inf
        self.conn = None
        self.cr = None

    def __del__(self):
        self.db_close()

    @classmethod
    def get_instance(cls, user_inf=OracleUser):
        if cls.instance:
            return cls.instance
        else:
            obj = cls(user_inf=user_inf)
            cls.instance = obj
            return obj

    def db_conn(self):
        if self.conn is not None:
            self.db_close()
        try:
            self.conn = cx_Oracle.connect(self.conn_inf)
        except Exception as e:
            print("Oracle connect failed")
            return self.conn, self.cr
        else:
            print("Oracle connect succeed")
            self.cr = self.conn.cursor()
            return self.conn, self.cr

    def db_close(self):
        if self.conn is not None:
            try:
                self.cr.close()
                self.conn.close()
            except Exception as e:
                print('db_close', e)
            finally:
                self.conn = None
                self.cr = None

    def call_oracle_data(self, sql, conn_inf=None, fram=None):
        """
        :param sql: 需要执行的SQL语句
        :param conn_inf:如果要更改链接地址
        :param fram: 是否返回DataFrame格式数据，为True则返回DataFrame
        :return: 默认返回元组格式数据
        """

        def tuple2frame(result, index):
            column_name = []
            for i in range(len(index)):
                index_name = index[i][0]
                column_name.append(index_name)
            result = pd.DataFrame(result, columns=column_name)
            return result

        if conn_inf is not None:
            self.conn_inf = conn_inf
            self.db_close()
        else:
            pass
        cr = self.cr
        conn = self.conn
        if conn is None:
            conn, cr = self.db_conn()
        if cr:
            try:
                cr.execute(sql)
            except Exception as e:
                conn.commit()
                print("call oracle error", e)
            else:
                index = cr.description
                result = cr.fetchall()
                conn.commit()
                if result is not None:
                    if fram is not None:
                        fresult = tuple2frame(result, index)
                        return fresult
                    else:
                        return result
                else:
                    return None
        else:
            print("database connect failed")
            # sys.exit(0)

    def send_oracle_data(self, sql, data=None, conn_inf=None, sql_delete=None, key_location=None):
        """
        :param sql: 插入数据SQL语句
        :param data: 插入数据，列表或元组等可迭代格式
        :param conn_inf: 更换链接信息？
        :param sql_delete: 需要删除数据的SQL语句
        :param key_location: 主键位置
        :return: 无返回
        """
        error_num = 0
        correct_num = 0
        repet_num = 0
        if conn_inf is not None:
            self.conn_inf = conn_inf
            self.db_close()
        else:
            pass
        cr = self.cr
        conn = self.conn
        if conn is None:
            conn, cr = self.db_conn()
        else:
            pass
        if cr:
            for i in data:
                try:
                    cr.execute(sql, i)
                except cx_Oracle.IntegrityError as e:
                    error_num += 1
                    repet_num += 1
                    # 主键去重
                    if sql_delete is not None and key_location is not None:
                        try:
                            conn.commit()
                            key = [i[m] for m in key_location]
                            key = tuple(key)
                            sql_delete2 = sql_delete % key
                            print(sql_delete2)
                            cr.execute(sql_delete2)
                            conn.commit()
                            cr.execute(sql, i)
                            conn.commit()
                        except Exception as e:
                            print('delete error', e)
                        else:
                            print('delete success!', e)
                except Exception as e:
                    print(i)
                    print("call oracle error", e)
                    error_num += 1
                else:
                    correct_num += 1
                finally:
                    conn.commit()
            print("插入成功:%s,插入失败:%s,其中主键重复：%s" % (correct_num, error_num, repet_num))
            # self.db_close()
        else:
            print("database connect failed")
            # return pd.DataFrame({})

    def send_oracle_data_many(self, sql, data=None, conn_inf=None):
        if conn_inf is not None:
            self.conn_inf = conn_inf
            self.db_close()
        else:
            pass
        cr = self.cr
        conn = self.conn
        if conn is None:
            conn, cr = self.db_conn()
        else:
            pass

        print(type(data))
        assert type(data) == list
        if cr:
            try:
                cr.executemany(sql, data)
            except Exception as e:
                print(e)
                conn.commit()
            finally:
                conn.commit()
                self.db_close()
        else:
            print("database connect failed")
            # return pd.DataFrame({})


if __name__ == '__main__':
    """
    样例
    单例模式：即整个项目只创建一个连接实例，若要多进程，多线程则加上锁
    """
    # 自定义连接信息
    pg_inf = {'database': "zkr", 'user': "postgres", 'password': "postgres",
              'host': "33.83.100.145", 'port': "5432"}
    PG = Postgres.get_instance()  # 单例模式 即，整个项目只创建一个连接实例，若要多进程，多线程则加上锁
    PG2 = Postgres.get_instance()  # 返回的实例内存指向PG
    PG.conn = '123'
    print(PG2.conn)
    PG3 = Postgres()  # 普通模式
    print(PG3.conn)

    # # 测试SQL
    # sql = "select * from ceshi"
    # sql2 = "insert into ceshi values({0})"
    # sql_delete = "delete from ceshi where d1='%s' and d4='%s'"
    # key_location = [0, 3]    # 主键位置参数
    #
    # result = PG.call_pg_data(sql, pg_inf, fram=True)     # 获取Dataframe格式结果
    # result = result.values
    # insert_pram = ['%s' for i in range(len(result[0]))]     # 根据插入数据量构造SQL %s参数
    # insert_pram= ','.join(insert_pram)
    # # print(insert_pram)
    # # 插入数据
    # PG.send_pg_data(sql2.format(insert_pram), result, pg_inf,sql_delete=sql_delete,key_location =key_location )
    # # db.send_pg_data_many(sql2.format(insert_pram), result, pg_inf)
    # print(result)
    # # 关闭数据库连接
    # PG.db_close()

    """
    Oracle表字段名不能为中文，否则数据插入不了
    Oracle中类型为Number格式的数据读出来会出错，python识别为nan-not a number
    """
    # sql3 = "select * from LANE_FUNCTION"
    # sql4 = "insert into LANE_FUNCTION_TEST values ({0})"
    # OracleUser2 = 'SIG_OPT_ADMIN/admin@192.168.20.56/orcl'
    # Ol = Oracle(OracleUser)
    # result = Ol.call_oracle_data(sql3,conn_inf=OracleUser2, fram=True)
    # result = result.values.tolist()
    # print(result)
    # insert_pram = [':'+str(i) for i in range(len(result[0]))]     # 根据插入数据量构造SQL %s参数
    # insert_pram = ','.join(insert_pram)
    # print(sql4.format(insert_pram))
    # Ol.send_oracle_data(sql4.format(insert_pram), conn_inf=OracleUser2,data=result)
    # Ol.db_close()   # 关闭数据库连接
