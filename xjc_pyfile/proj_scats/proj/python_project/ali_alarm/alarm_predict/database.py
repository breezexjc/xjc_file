import psycopg2
import cx_Oracle
# from ..conf import db_inf
#from scats.interface.conf import db_inf

# pg_inf = {'database': "signal_specialist",'user': "postgres",'password': "postgres",
#           'host': "192.168.20.46",'port': "5432"}

# demo_pg_inf = {'database': "zkr",'user': "postgres",'password': "postgres",
#           'host': "192.168.20.56",'port': "5432"}

demo_pg_inf = {'database': "signal_specialist",'user': "postgres",'password': "postgres",
          'host': "33.83.100.145",'port': "5432"}

OracleUser = 'SIG_OPT_ADMIN/admin@192.168.20.56/orcl'

# demo_pg_inf = {'database': "signal_specialist",'user': "postgres",'password': "postgres",
#           'host': "192.168.20.46",'port': "5432"}

class Postgres(object):

    def __init__(self, pg_inf=demo_pg_inf):
        self.pg_inf = pg_inf
        self.ip = self.pg_inf['host']
        self.database = self.pg_inf['database']
        self.port = self.pg_inf['port']
        self.account = self.pg_inf['user']
        self.password = self.pg_inf['user']
        self.conn = None
        self.cr = None

    def db_conn(self):
        try:

            self.conn = psycopg2.connect(database=self.database,user=self.account,password=self.password,
                                    host=self.ip,
                                    port=self.port)
        except Exception as e:
            print(self.ip +"Postgres connect failed")
            return self.conn, self.cr
        else:
            # print(self.ip +"Postgres connect succeed")
            self.cr = self.conn.cursor()
            return self.conn ,self.cr

    def db_close(self):
        if self.conn:
            self.cr.close()
            self.conn.close()

class Oracle(object):
    user_inf = OracleUser

    def __init__(self):
        self.conn_inf = Oracle.user_inf
        self.conn = None
        self.cr = None
    def db_conn(self):
        try:
            self.conn = cx_Oracle.connect(self.conn_inf)
        except Exception as e:
            print("Oracle connect failed")

        else:
            print("Oracle connect succeed")
            self.cr = self.conn.cursor()


    def db_close(self):
        if self.conn:
            self.cr.close()
            self.conn.close()

