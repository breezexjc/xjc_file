#
# from alarm_priority_algorithm2.database import Postgres
# pg_inf = {'database': "inter_info",'user': "postgres",'password': "postgres",
#           'host': "192.168.20.46",'port': "5432"}
#
# sql = "select * from dict_group"
# sql2 = "insert into dict_group values(%s,%s,%s,%s,%s,%s)"
# db = Postgres()
# result = db.call_pg_data(sql, pg_inf)
# result = result.values
# db.send_pg_data(sql2, result, pg_inf)
# db.send_pg_data_many(sql2, result, pg_inf)
# print(result)
