import psycopg2
import cx_Oracle
# from ..conf import db_inf
# from scats.interface.conf import db_inf

# pg_inf = {'database': "signal_specialist",'user': "postgres",'password': "postgres",
#           'host': "192.168.20.56",'port': "5432"}

#
# pg_inf = {'database': "research",'user': "postgres",'password': "postgres",
#           'host': "192.168.20.45",'port': "5432"}

#
# pg_inf = {'database': "signal_specialist",'user': "postgres",'password': "postgres",
#           'host': "192.168.20.46",'port': "5432"}
pg_inf = {'database': "signal_specialist",'user': "postgres",'password': "postgres",
          'host': "33.83.100.145",'port': "5432"}

class Postgres(object):

    pg_inf = pg_inf
    ip = pg_inf['host']
    database = pg_inf['database']
    port = pg_inf['port']
    account = pg_inf['user']
    password = pg_inf['user']

    def __init__(self):
        self.ip = Postgres.ip
        self.database = Postgres.database
        self.port = Postgres.port
        self.account = Postgres.account
        self.password = Postgres.password
        self.conn = None
        self.cr = None

    def db_conn(self):
        try:

            self.conn = psycopg2.connect(database=self.database,user=self.account,password=self.password,
                                    host=self.ip,
                                    port=self.port)
        except Exception as e:
            print(self.ip +"Postgres connect failed")

        else:
            # print(self.ip +"Postgres connect succeed")
            self.cr = self.conn.cursor()


    def db_close(self):
        if self.conn:
            self.cr.close()
            self.conn.close()


# class Oracle(object):
#     user_inf = db_inf.OracleUser
#
#     def __init__(self):
#         self.conn_inf = Oracle.user_inf
#         self.conn = None
#         self.cr = None
#     def dbconn(self):
#         try:
#             self.conn = cx_Oracle.connect(self.conn_inf)
#         except Exception as e:
#             print("Oracle connect failed")
#
#         else:
#             print("Oracle connect succeed")
#             self.cr = self.conn.cursor()
#
#
#     def dbclose(self):
#         if self.conn:
#             self.cr.close()
#             self.conn.close()
#
