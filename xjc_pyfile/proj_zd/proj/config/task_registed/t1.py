import datetime as dt
import time
import os
from multiprocessing import Process, Pipe
import threading
import psycopg2
import sys
sys.path.append(r"H:\program\python_demo")
# from ActiveMQ import Message
import json


def task(pid):
    with open('abc.txt', 'w') as f:
        print(dt.datetime.now(), file=f)
    print(dt.datetime.now())
    pid = os.getpid()
    timer = threading.Timer(2.0, task, args=[pid])
    timer.start()
    # Message.send_to_queue(pid)
    # with open( sys.path[0]+'\\'+'')


def main():
    pid = os.getpid()
    data = {"task": "t1", "pid": pid}
    json_str = json.dumps(data)
    timer = threading.Timer(2.0, task, args=[json_str])
    timer.start()
    timer.join()


if __name__=="__main__":
    main()

