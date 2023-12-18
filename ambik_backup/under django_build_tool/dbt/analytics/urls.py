from django import urls
from django.contrib import admin
from django.urls import path
from django.urls.conf import include
from .import views
from django.conf import settings
from rest_framework.routers import DefaultRouter, SimpleRouter

#app_name for food's urls.py
# app_name= 'food'
from django.urls import path
from django.conf.urls.static import static

urlpatterns = [
    path('python_index/', views.python_logs_index, name='python_index'),
   
     #GitRepo item


] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)




