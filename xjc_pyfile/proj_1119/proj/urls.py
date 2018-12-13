from django.conf.urls import *
# from django.contrib import admin
from . controller import search, Interface
from . controller import schedular_task
# admin.autodiscover()

# 注册路由
urlpatterns = [
    # url(r'^getJson$', search.getJson),
    url(r'^getJson1$', search.getJson1),
    url(r'^startInterface$', Interface.getJson2),
    url(r'^getOperate$', Interface.getOperate),
    # url(r'^admin/', admin.site.urls),
    url(r'^getAlarmAuto$', Interface.getAlarmAuto),
    url(r'^demo/guest/getState$', Interface.demo),
    # url(r'^taskOperate$', schedular_task.getScheduler),
    url(r'^demo/scats/getInterface$', Interface.getInterfaceStatus),
    url(r'^demo/scats/getParseFailed$', Interface.getParseFailed),
    url(r'^demo/scats/getScheTask$', Interface.getScheTask),
    url(r'^demo/scats/getLaneStatus$', Interface.getLaneStatus),
    url(r'^demo/scats/getRequestManage$', Interface.getRequestManage),
]