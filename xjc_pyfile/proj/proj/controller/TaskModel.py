import re,os
from importlib import import_module
from multiprocessing import Process
import sys


class TaskRegister(object):
    def __init__(self, task, args=None):
        import sys
        # sys.path.insert(0, r"H:\program\proj\proj\config\task_registed")
        self.task_name = task
        self.args = args
        # self.task_pid = None
        self.task_pid = os.getpid()
        self.process = None
        self.task_state = {"TaskName": self.task_name, "TaskPid": self.task_pid, "TaskStatus":None}

    def get_registed_task(self):
        with open(r'..\task_registed\task_list.txt', 'r') as f:
            line_data = f.readline()
            split_data = re.split(r'[: ]', line_data, )
            task_name = [i for i in split_data if i != 'taskname' and i != '']
            # split_data = line_data.split('taskname:')
        print(task_name)
        return task_name

    def start_task(self):
        print("parent process %s" % (os.getpid()))
        print(self.task_name)
        current_dir = os.getcwd()
        last_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        sys.path.insert(0, last_dir+r"\config\task_registed")
        try:
            run_task = import_module(self.task_name)
        except Exception as e:
            print(e)
            print("can't find task")
        else:
            print("run_task:", run_task)
            if self.args:
                p = Process(target=run_task.main, args=(self.args))
                # p.daemon = True
                p.start()
                self.process = p
                # run_task.main(args)
            else:
                p = Process(target=run_task.main, args=())
                p.daemon = True
                p.start()
                self.process = p
                # run_task.main()

    def stop_task(self):
        if self.process:
            self.process.terminate()

    def restart_task(self):
        if self.process:
            self.process.terminate()
            self.start_task()

    def check_status(self):
        if self.process is None:
            pass
        else:
            self.task_state = {"TaskName": self.task_name, "TaskPid": self.task_pid, "TaskStatus": self.process.is_alive()}

            # return self.process.is_alive()


if __name__ == '__main__':
    import time
    # path = sys.path[0]
    # print(sys.path)
    # print(os.path.abspath(path))
    p = TaskRegister('t1')
    p.start_task()
    while True:
        p.check_status()
        print(p.task_state)
        time.sleep(2)



    # print('***获取当前目录***')
    # print(os.getcwd())
    # print(os.path.abspath(os.path.dirname(__file__)))
    # print('***获取上级目录***')
    # print(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
    # print(os.path.abspath(os.path.dirname(os.getcwd())))
    # print(os.path.abspath(os.path.join(os.getcwd(), "..")))
    # print( '***获取上上级目录***')
    # print(os.path.abspath(os.path.join(os.getcwd(), "../..")))