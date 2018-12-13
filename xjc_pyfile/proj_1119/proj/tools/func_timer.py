import datetime as dt

def timer(func):
    def warpper(*args, **kwargs):
        stime = dt.datetime.now()
        res = func(*args, **kwargs)
        etime = dt.datetime.now()
        print("[DeBug] {0} CostTime: {1} ".format(func.__name__, (etime - stime).seconds))
        return res

    return warpper