#!/usr/bin/env python
import os
import sys
sys.path.append(os.getcwd())
import sys
import signal


def quit(signal,SIGINT):
    """
    CTRL+C退出程序，执行清理操作
    """
    from django_apscheduler.jobstores import DjangoJobStore
    print('stop fusion')
    djs = DjangoJobStore()
    djs.remove_all_jobs()
    sys.exit()

signal.signal(signal.SIGINT, quit)
signal.signal(signal.SIGTERM, quit)

if __name__ == '__main__':
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proj.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)
