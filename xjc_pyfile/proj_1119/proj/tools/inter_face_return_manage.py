import datetime as dt
import logging
from app_manage.models import InterfaceRequestRecord
import json

Log =logging.getLogger('scripts')
# 用于获取接口请求状态信息并存储
def inter_face_manage(func):

    def warper(request):
        reponse = None
        if 'HTTP_X_FORWARDED_FOR' in request.META:
            ip = request.META['HTTP_X_FORWARDED_FOR']
        else:
            ip = request.META['REMOTE_ADDR']
        start_time = dt.datetime.now()
        try:
            reponse = func(request)
        except Exception as e:
            reponse_id = '500'
            message_length = 0
        else:
            reponse_id = '200'
            message = json.loads(reponse.content)
            message_length = len(message['result'])
        inter_face_name = func.__name__
        end_time = dt.datetime.now()
        request_delta = (end_time-start_time).seconds
        print('inter_face_name:'+inter_face_name +' cost time(s): %s' % request_delta  +'IP:%s'%ip)
        Log.info('inter_face_name:'+inter_face_name +' cost time(s): %s' % request_delta +'IP:%s'%ip)
        InterfaceRequestRecord.objects.create(interface_name=inter_face_name, request_time=start_time,return_time=end_time,
                                              cost_time=request_delta,reponse_id=reponse_id,message_length=message_length,ip=ip)
        return reponse
    return warper


# 用于计算函数运行时间
