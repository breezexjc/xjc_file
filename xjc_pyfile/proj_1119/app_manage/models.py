from django.db import models

# Create your models here.

class InterfaceRequestRecord(models.Model):
    interface_name = models.CharField(max_length=30,verbose_name="接口名称")
    request_time = models.DateTimeField(verbose_name="请求时间")
    return_time = models.DateTimeField(verbose_name="返回时间")
    cost_time = models.IntegerField(verbose_name="花费时间")
    reponse_id = models.IntegerField(verbose_name="返回http id")
    message_length = models.IntegerField(verbose_name="返回数据长度")
    ip = models.CharField(max_length=30,verbose_name="ip地址")

