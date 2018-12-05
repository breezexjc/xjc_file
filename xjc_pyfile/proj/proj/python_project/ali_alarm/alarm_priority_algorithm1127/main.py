from proj.python_project.ali_alarm.alarm_priority_algorithm1126.alarm_data_regular_filter import create_process,new_kde_cal,his_model_update
import time
from proj.python_project.ali_alarm.alarm_priority_algorithm1127.alarm_auto_dispose import OperateAutoDis

if __name__ == '__main__':
    # his_model_update()
    # create_process()
    # while True:
    #     time.sleep(15)
    # result = new_kde_cal()
    # %%
    # result2 = new_kde_cal()
    O1 = OperateAutoDis()
    # O1.get_alarm_operate_type()
    result = O1.alarm_auto_judge(['14KVJ097OO0'])
    result2 = O1.alarm_auto_judge(['14KVJ097OO0'])
    print(result)

