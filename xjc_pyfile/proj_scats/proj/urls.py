from django.conf.urls import *
from django.contrib import admin
from . controller import search, Interface,schedular_task

# 注册路由
urlpatterns = [
    # url(r'^getJson$', search.getJson),
    url(r'^getJson1$', search.getJson1),
    # url(r'^startInterface$', Interface.getJson2),
    # url(r'^getOperate$', Interface.getOperate),
    # url(r'^admin/', admin.site.urls),
    # url(r'^getOperateRatio/', Interface.getOperateRatio),
    # url(r'^runOperate$', Interface.runOperate)
]