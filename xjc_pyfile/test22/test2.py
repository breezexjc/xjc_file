from database import  Oracle,ConnectInf
import threading
lock = threading.Lock()

def insert_data():
    O = Oracle(ConnectInf.OracleUser_Enjoyor)
    result = O.call_oracle_data("select * from HZ_SCATS_OUTPUT where fstr_date = to_date('2018-09-27','yyyy-MM-dd') "
                                "and FSTR_INTERSECTID in (25,16,28,95,41,36)")
    # result = O.call_oracle_data("select * from HZ_SCATS_OUTPUT where fstr_date = to_date('2018-09-27','yyyy-MM-dd') "
    #                             "and FSTR_INTERSECTID in (684,138,168,167,138,98)")
    # O.send_oracle_data("insert into HZ_SCATS_OUTPUT_TEST VALUES (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)",result)
    lock.acquire()
    try:
        for i in result:
            try:
                print(i)
                O.cr.execute("insert into HZ_SCATS_OUTPUT_TEST VALUES (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)",i)
            except Exception as e:
                O.conn.commit()
                print(e)
            else:
                O.conn.commit()
    except Exception as e:
        pass
    finally:
        lock.release()



if __name__ =='__main__':
    threads = []
    for i in range(5):
        name = 'thread' + str(i)
        locals()[name] = threading.Thread(target=insert_data, name='get_data',
                                          args=[])
        print('thread %s is running...' % i)
        locals()[name].start()
        threads.append(locals()[name])
        # print(threading.current_thread().name)
        # n += 1
        # thread.join()
    for t in threads:
        t.join()
