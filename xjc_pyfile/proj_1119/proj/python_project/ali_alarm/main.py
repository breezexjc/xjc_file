from .alarm_priority_algorithm2 import alarm_data_regular_filter
import datetime as dt
def main():
    time = dt.datetime.now()
    alarm_data_regular_filter.create_process()

if __name__ == '__main__':
    # from alarm_priority_algorithm2 import alarm_data_regular_filter
    main()