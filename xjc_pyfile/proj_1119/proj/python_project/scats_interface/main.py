from proj.python_project.scats_interface.data_request_check import InterfaceCheck
from proj.python_project.scats_interface.condition_monitor import InterfaceStatus


if __name__ == "__main__":
    I = InterfaceStatus()
    I.salk_send(15)
    I2 = InterfaceCheck()
    I.parsing_failed_check("战略运行记录接口",)
    # I.salk_list_request()
    # I.operate_request()
