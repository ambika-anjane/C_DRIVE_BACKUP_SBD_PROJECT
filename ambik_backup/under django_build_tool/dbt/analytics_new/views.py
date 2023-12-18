from django_celery_beat.models import IntervalSchedule, CrontabSchedule
from rest_framework.viewsets import ModelViewSet
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from dbt.utils.common import load_dbt_current_version
from config.celery_app import dbt_runner_task,python_runner_task
from django.template import context, loader
from rest_framework.response import Response
from django.shortcuts import redirect, render
from django.http import HttpResponse





from dbt.analytics.models import (
    GitRepo,
    ProfileYAML,
    SSHKey,
    PeriodicTask as PeriodicTaskModel,
)
from dbt.analytics.serializers import (
    GitRepoSerializer,
    IntervalScheduleSerializer,
    PeriodicTaskSerializer,
    ProfileYAMLSerializer,
    SSHKeySerializer,
    WritePeriodicTaskSerializer,
    CrontabScheduleSerializer,
    DBTCurrentVersionSerializer,
    RunTaskSerializer
)


# class GitRepoAPIViewset(ModelViewSet):
#     http_method_names = ["get", "post", "delete", "head", "options", "trace"]
#     queryset = GitRepo.objects.all()
#     serializer_class = GitRepoSerializer

def index(request):
    item_list= GitRepo.objects.all()
    template= loader.get_template('admin/base_site.html')
    context={
        'itemList': item_list,
    }
    return HttpResponse(template.render(context, request))  