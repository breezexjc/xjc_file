# from proj.controller.schedular_task import clear_database
import datetime as dt
from proj.config.database import Postgres
from proj.config.sql_text import SqlText

def clear_database():
    pg = Postgres()
    save_date = (dt.datetime.now() - dt.timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    pg.execute(SqlText.sql_delete_real_phase.format(save_date))
    pg.execute(SqlText.sql_delete_kde_vaue.format(save_date))
    pg.db_close()
    print("数据库清理完成")

clear_database()