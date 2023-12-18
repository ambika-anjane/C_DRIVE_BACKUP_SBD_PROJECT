from django_celery_beat.models import IntervalSchedule, CrontabSchedule
from rest_framework.viewsets import ModelViewSet
from rest_framework.views import APIView
from django.views.generic import ListView
from rest_framework.response import Response
from rest_framework import status
from dbt.utils.common import load_dbt_current_version
from config.celery_app import dbt_runner_task,python_runner_task
from django.template import context, loader
from rest_framework.response import Response
from django.shortcuts import redirect, render
from django.http import HttpResponse
from django.http import HttpResponse, HttpResponseRedirect

from dbt.analytics.models import (
    GitRepo,
    ProfileYAML,
    SSHKey,
    PeriodicTask as PeriodicTaskModel,
    PythonLogs,
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


def python_logs_index(request):
    item_list= PythonLogs.objects.all()
    template= loader.get_template('admin/base.html')
    change_list_template = 'admin/analytics/change_list.html'

    context={
        'itemList': item_list,
    }
    return HttpResponse(template.render(context, request))

class GitRepoAPIViewset(ModelViewSet):
    http_method_names = ["get", "post", "delete", "head", "options", "trace"]
    queryset = GitRepo.objects.all()
    serializer_class = GitRepoSerializer

class PostYMALDetailsView(ModelViewSet):
    serializer_class = ProfileYAMLSerializer
    queryset = ProfileYAML.objects.all()

class SSHKeyViewSets(ModelViewSet):
    http_method_names = ["get", "post", "delete", "head", "options", "trace"]
    queryset = SSHKey.objects.all()
    serializer_class = SSHKeySerializer


class InterValViewSet(ModelViewSet):
    queryset = IntervalSchedule.objects.all()
    serializer_class = IntervalScheduleSerializer


class CrontabScheduleViewSet(ModelViewSet):
    queryset = CrontabSchedule.objects.all()
    serializer_class = CrontabScheduleSerializer


class AddPeriodicTask(ModelViewSet):
    queryset = PeriodicTaskModel.objects.all()
    serializer_class = WritePeriodicTaskSerializer

    def get_serializer_class(self):
        if self.request.method == "POST":
            return self.serializer_class
        else:
            return PeriodicTaskSerializer
        



class DBTCurrentVersionView(APIView):
    def get(self, request,):
        modules_version_data = load_dbt_current_version()
        serializer = DBTCurrentVersionSerializer(data=modules_version_data, many=True)
        serializer.is_valid(raise_exception=True)
        return Response(serializer.data)


class RunDBTTask(APIView):
    serializer_class = RunTaskSerializer

    def post(self, request, *args, **kwargs):
        serializer = RunTaskSerializer(data=request.data)
        if serializer.is_valid():
            task_id = serializer.validated_data["task_id"]
            task = PeriodicTaskModel.objects.get(id=task_id)
            args = eval(task.args) if task.args else []
            kwargs = eval(task.kwargs) if task.kwargs else {}
            dbt_runner_task.delay(*args, **kwargs)
            return Response(
                {"status": "DBT Task has been initiated"}, status=status.HTTP_200_OK
            )
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
class RunPythonTask(APIView):
    serializer_class = RunTaskSerializer

    def post(self, request, *args, **kwargs):
        serializer = RunTaskSerializer(data=request.data)
        if serializer.is_valid():
            task_id = serializer.validated_data["task_id"]
            task = PeriodicTaskModel.objects.get(id=task_id)
            args = eval(task.args) if task.args else []
            kwargs = eval(task.kwargs) if task.kwargs else {}
            python_runner_task.delay(*args, **kwargs)
            return Response(
                {"status": "Python Task has been initiated"}, status=status.HTTP_200_OK
            )
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


 