import psycopg2
from ..config.jdbcConfig import *

class TestModelTestMappper:

    def addOneRecord(self,vo):
        name=vo.name
        sql="INSERT INTO TestModel_test ( name) VALUES ('"+name+"');"
        sql="INSERT INTO TestModel_test ( name) VALUES (%s);"
        print(sql)
        conn = psycopg2.connect(database=dataBase, user=user, password=password, host=host, port=port)
        cur = conn.cursor()
        try:
            cur.execute(sql,(name,))
            reuslt=cur.fetchall()
            print(reuslt)
            conn.commit()
        except Exception as e:
            print(e)
        finally:
            conn.commit()
            cur.close()
            conn.close()
        cur.close()
        conn.close()